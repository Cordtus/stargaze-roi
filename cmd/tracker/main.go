// Package main is the entry point for the Stargaze ROI Tracker.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/cordt-sei/starmos-roi-tracker/internal/chain"
	"github.com/cordt-sei/starmos-roi-tracker/internal/config"
	"github.com/cordt-sei/starmos-roi-tracker/internal/db"
	"github.com/cordt-sei/starmos-roi-tracker/internal/price"
	"github.com/cordt-sei/starmos-roi-tracker/internal/processor"
	"github.com/cordt-sei/starmos-roi-tracker/internal/server"
)

func main() {
	// Parse flags
	configPath := flag.String("config", "config.toml", "Path to configuration file")
	serverOnly := flag.Bool("server-only", false, "Run only the HTTP server without event processing")
	generateJSON := flag.String("generate-json", "", "Generate stats JSON to specified path and exit")
	resetDB := flag.Bool("reset-db", false, "Drop and recreate the roi_tracker schema (fresh start)")
	flag.Parse()

	// Setup logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// Load configuration
	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Error("failed to load config", "error", err, "path", *configPath)
		os.Exit(1)
	}

	logger.Info("loaded configuration",
		"chain_id", cfg.Chain.ChainID,
		"target_usd", cfg.Target.USDAmount,
		"contracts", len(cfg.Contract.Addresses),
	)

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to shared PostgreSQL database
	pool, err := pgxpool.New(ctx, cfg.Database.ConnString)
	if err != nil {
		logger.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		logger.Error("failed to ping database", "error", err)
		os.Exit(1)
	}

	logger.Info("connected to database")

	// Reset DB if requested (drop and recreate schema)
	if *resetDB {
		logger.Info("resetting roi_tracker schema (--reset-db)")
		if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS roi_tracker CASCADE"); err != nil {
			logger.Error("failed to drop schema", "error", err)
			os.Exit(1)
		}
		logger.Info("schema dropped")
	}

	// Initialize ROI tracker schema
	if err := db.InitSchema(ctx, pool); err != nil {
		logger.Error("failed to initialize schema", "error", err)
		os.Exit(1)
	}

	logger.Info("database schema initialized")

	// If generate-json mode, create stats file and exit
	if *generateJSON != "" {
		if err := generateStatsJSON(ctx, pool, cfg, *generateJSON); err != nil {
			logger.Error("failed to generate stats JSON", "error", err)
			os.Exit(1)
		}
		logger.Info("stats JSON generated", "path", *generateJSON)
		return
	}

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Create price fetcher
	priceFetcher := price.NewFetcher(
		cfg.CoinGecko.APIBase,
		cfg.CoinGecko.HistoricalAPIKey,
		cfg.CoinGecko.RateLimitPerMin,
		pool,
		logger.With("component", "price"),
	)

	// Start price backfill in background (paced within CoinGecko rate limit)
	go func() {
		grantDate := cfg.GrantDateParsed()
		if grantDate.IsZero() {
			grantDate = time.Date(2025, 8, 1, 0, 0, 0, 0, time.UTC) // Before earliest contract
		}
		fetched, err := priceFetcher.Backfill(ctx, grantDate, time.Now())
		if err != nil {
			logger.Warn("price backfill incomplete", "error", err, "fetched", fetched)
		} else {
			logger.Info("price backfill complete", "days_fetched", fetched)
		}
	}()

	// Create and start HTTP server
	srv := server.New(cfg, pool, logger.With("component", "server"), nil)
	go func() {
		if err := srv.Start(); err != nil {
			logger.Error("HTTP server error", "error", err)
			cancel()
		}
	}()

	// Start event processor unless server-only mode
	if !*serverOnly {
		if len(cfg.Contract.Addresses) == 0 {
			logger.Warn("no seed contract addresses configured, processor will not run")
		} else {
			// Connect to chain via gRPC
			chainClient, err := chain.NewClient(cfg.Chain.GRPCEndpoint, logger.With("component", "chain"))
			if err != nil {
				logger.Error("failed to connect to gRPC", "error", err, "endpoint", cfg.Chain.GRPCEndpoint)
				os.Exit(1)
			}
			defer chainClient.Close()

			// Run contract discovery (expands seeds via code_id + creator, caches in DB)
			refreshInterval := time.Duration(cfg.Discovery.RefreshMinutes) * time.Minute
			disc := chain.NewDiscovery(
				chainClient, pool,
				logger.With("component", "discovery"),
				refreshInterval,
			)
			if err := disc.Run(ctx, cfg.Contract.Addresses); err != nil {
				logger.Error("contract discovery failed", "error", err)
				os.Exit(1)
			}

			logger.Info("contract discovery complete", "total_contracts", disc.ContractCount())

			proc := processor.New(pool, chainClient, disc, priceFetcher, cfg, logger.With("component", "processor"))
			go func() {
				if err := proc.Start(ctx); err != nil {
					logger.Error("processor error", "error", err)
				}
			}()
		}
	}

	// Wait for shutdown
	select {
	case sig := <-sigCh:
		logger.Info("received shutdown signal", "signal", sig)
	case <-ctx.Done():
	}

	// Graceful shutdown
	logger.Info("shutting down...")
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("server shutdown error", "error", err)
	}

	logger.Info("shutdown complete")
}

// statsJSON is the structure written to the static JSON file.
type statsJSON struct {
	TargetUSD        string   `json:"target_usd"`
	TotalFeesUSD     string   `json:"total_fees_usd"`
	TotalFeesAtom    string   `json:"total_fees_atom"`
	TransactionCount int64    `json:"transaction_count"`
	ProgressPercent  string   `json:"progress_percent"`
	AvgAtomPriceUSD  string   `json:"avg_atom_price_usd"`
	YearsToBreakeven *float64 `json:"years_to_breakeven"`
	FirstTxTimestamp *string  `json:"first_tx_timestamp"`
	LastTxTimestamp  *string  `json:"last_tx_timestamp"`
	UpdatedAt        string   `json:"updated_at"`
	ChainID          string   `json:"chain_id"`
	ContractInfo     string   `json:"contract_info"`
	ProposalID       int      `json:"proposal_id,omitempty"`
	GrantAtom        string   `json:"grant_atom,omitempty"`
	GeneratedAt      string   `json:"generated_at"`
}

// generateStatsJSON queries the database and writes stats to a JSON file.
func generateStatsJSON(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, outPath string) error {
	stats, err := db.GetStats(ctx, pool)
	if err != nil {
		return err
	}

	target := cfg.TargetUSD()
	var progressPercent decimal.Decimal
	if target.GreaterThan(decimal.Zero) {
		progressPercent = stats.TotalUSDBurned.Div(target).Mul(decimal.NewFromInt(100))
	}
	totalAtom := stats.TotalUatomBurned.Div(decimal.NewFromInt(1_000_000))

	out := statsJSON{
		TargetUSD:        target.StringFixed(2),
		TotalFeesUSD:     stats.TotalUSDBurned.StringFixed(10),
		TotalFeesAtom:    totalAtom.StringFixed(6),
		TransactionCount: stats.TransactionCount,
		ProgressPercent:  progressPercent.StringFixed(10),
		AvgAtomPriceUSD:  stats.AvgAtomPriceUSD.StringFixed(6),
		UpdatedAt:        stats.LastUpdated.Format(time.RFC3339),
		ChainID:          stats.ChainID,
		ContractInfo:     stats.ContractAddress,
		ProposalID:       cfg.Target.ProposalID,
		GrantAtom:        cfg.GrantAtom().StringFixed(6),
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
	}

	// Calculate years to break even
	if stats.FirstBurnTimestamp != nil && stats.TotalUSDBurned.GreaterThan(decimal.Zero) {
		daysSinceFirst := time.Since(*stats.FirstBurnTimestamp).Hours() / 24
		if daysSinceFirst > 0 {
			dailyRate, _ := stats.TotalUSDBurned.Div(decimal.NewFromFloat(daysSinceFirst)).Float64()
			if dailyRate > 0 {
				remaining, _ := target.Sub(stats.TotalUSDBurned).Float64()
				years := remaining / (dailyRate * 365.25)
				out.YearsToBreakeven = &years
			}
		}
	}

	if stats.FirstBurnTimestamp != nil {
		ts := stats.FirstBurnTimestamp.Format(time.RFC3339)
		out.FirstTxTimestamp = &ts
	}
	if stats.LastBurnTimestamp != nil {
		ts := stats.LastBurnTimestamp.Format(time.RFC3339)
		out.LastTxTimestamp = &ts
	}

	data, err := json.MarshalIndent(out, "", "\t")
	if err != nil {
		return err
	}

	return os.WriteFile(outPath, data, 0644)
}
