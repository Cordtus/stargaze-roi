// Package server provides the HTTP server and API endpoints.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/cordt-sei/starmos-roi-tracker/internal/config"
	"github.com/cordt-sei/starmos-roi-tracker/internal/db"
)

// Server handles HTTP requests.
type Server struct {
	cfg    *config.Config
	pool   *pgxpool.Pool
	logger *slog.Logger
	server *http.Server
}

// New creates a new HTTP server.
func New(cfg *config.Config, pool *pgxpool.Pool, logger *slog.Logger, staticFS fs.FS) *Server {
	s := &Server{
		cfg:    cfg,
		pool:   pool,
		logger: logger,
	}

	mux := http.NewServeMux()

	// API routes
	mux.HandleFunc("GET /api/stats", s.handleStats)
	mux.HandleFunc("GET /api/breakdown", s.handleBreakdown)
	mux.HandleFunc("GET /api/transactions", s.handleTransactions)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /api/health", s.handleHealth)

	// Static files
	if staticFS != nil {
		mux.Handle("/", http.FileServer(http.FS(staticFS)))
	} else {
		mux.Handle("/", http.FileServer(http.Dir(cfg.Server.StaticDir)))
	}

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.Port),
		Handler:      s.withMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

// Start starts the HTTP server.
func (s *Server) Start() error {
	s.logger.Info("starting HTTP server", "port", s.cfg.Server.Port)
	return s.server.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}

// withMiddleware wraps the handler with logging and CORS middleware.
func (s *Server) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// CORS headers for API access
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		// Prevent caching of static files during development
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		// Wrap response writer to capture status code
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrapped, r)

		// Log request
		s.logger.Debug("request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration", time.Since(start),
		)
	})
}

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *responseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// StatsResponse is the response for the /api/stats endpoint.
type StatsResponse struct {
	TargetUSD            string   `json:"target_usd"`
	GrantAtom            string   `json:"grant_atom,omitempty"`
	GrantPriceUSD        string   `json:"grant_price_usd,omitempty"`
	TotalFeesUSDCurrent  string   `json:"total_fees_usd_current"`
	TotalFeesUSDHist     string   `json:"total_fees_usd_historical"`
	TotalFeesAtom        string   `json:"total_fees_atom"`
	TransactionCount     int64    `json:"transaction_count"`
	PricedTxCount        int64    `json:"priced_tx_count"`
	ProgressPercent      string   `json:"progress_percent"`
	AvgAtomPriceUSD      string   `json:"avg_atom_price_usd"`
	CurrentAtomPriceUSD  string   `json:"current_atom_price_usd,omitempty"`
	YearsToBreakeven     *float64 `json:"years_to_breakeven"`
	FirstTxTimestamp     *string  `json:"first_tx_timestamp,omitempty"`
	LastTxTimestamp       *string `json:"last_tx_timestamp,omitempty"`
	UpdatedAt            string   `json:"updated_at"`
	LastProcessedID      int64    `json:"last_processed_event_id"`
	ContractInfo         string   `json:"contract_info,omitempty"`
	ChainID              string   `json:"chain_id"`
	ProposalID           int      `json:"proposal_id,omitempty"`
	MultisigAddress      string   `json:"multisig_address,omitempty"`
}

// handleStats returns the current fee revenue statistics.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := db.GetStats(r.Context(), s.pool)
	if err != nil {
		s.logger.Error("failed to get stats", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	target := s.cfg.TargetUSD()

	// Convert uatom to ATOM for display
	totalAtom := stats.TotalUatomBurned.Div(decimal.NewFromInt(1_000_000))

	// Current-price valuation: total ATOM fees * latest cached price
	var currentPriceUSD decimal.Decimal
	var currentValuation decimal.Decimal
	now := time.Now().UTC().Truncate(24 * time.Hour)
	if price, found, err := db.GetCachedPrice(r.Context(), s.pool, now); err == nil && found {
		currentPriceUSD = price
		currentValuation = totalAtom.Mul(price)
	}

	// Use current valuation for progress if historical isn't available yet
	progressValue := stats.TotalUSDBurned
	if progressValue.IsZero() && currentValuation.GreaterThan(decimal.Zero) {
		progressValue = currentValuation
	}

	var progressPercent decimal.Decimal
	if target.GreaterThan(decimal.Zero) && progressValue.GreaterThan(decimal.Zero) {
		progressPercent = progressValue.Div(target).Mul(decimal.NewFromInt(100))
	}

	resp := StatsResponse{
		TargetUSD:           target.StringFixed(2),
		GrantAtom:           s.cfg.GrantAtom().StringFixed(6),
		GrantPriceUSD:       s.cfg.Target.GrantPriceUSD,
		TotalFeesUSDCurrent: currentValuation.StringFixed(2),
		TotalFeesUSDHist:    stats.TotalUSDBurned.StringFixed(2),
		TotalFeesAtom:       totalAtom.StringFixed(6),
		TransactionCount:    stats.TransactionCount,
		PricedTxCount:       stats.PricedTxCount,
		ProgressPercent:     progressPercent.StringFixed(10),
		AvgAtomPriceUSD:     stats.AvgAtomPriceUSD.StringFixed(6),
		UpdatedAt:           stats.LastUpdated.Format(time.RFC3339),
		LastProcessedID:     stats.LastProcessedID,
		ContractInfo:        stats.ContractAddress,
		ChainID:             stats.ChainID,
		ProposalID:          s.cfg.Target.ProposalID,
		MultisigAddress:     s.cfg.Target.MultisigAddress,
	}

	if currentPriceUSD.GreaterThan(decimal.Zero) {
		resp.CurrentAtomPriceUSD = currentPriceUSD.StringFixed(6)
	}

	if stats.FirstBurnTimestamp != nil {
		ts := stats.FirstBurnTimestamp.Format(time.RFC3339)
		resp.FirstTxTimestamp = &ts
	}
	if stats.LastBurnTimestamp != nil {
		ts := stats.LastBurnTimestamp.Format(time.RFC3339)
		resp.LastTxTimestamp = &ts
	}

	// Calculate years to break even based on daily fee revenue rate
	if stats.FirstBurnTimestamp != nil && progressValue.GreaterThan(decimal.Zero) {
		daysSinceFirst := time.Since(*stats.FirstBurnTimestamp).Hours() / 24
		if daysSinceFirst > 0 {
			dailyRate, _ := progressValue.Div(decimal.NewFromFloat(daysSinceFirst)).Float64()
			if dailyRate > 0 {
				remaining, _ := target.Sub(progressValue).Float64()
				years := remaining / (dailyRate * 365.25)
				resp.YearsToBreakeven = &years
			}
		}
	}

	s.writeJSON(w, resp)
}

// TransactionsResponse is the response for the /api/transactions endpoint.
type TransactionsResponse struct {
	Transactions []TransactionItem `json:"transactions"`
	Total        int64             `json:"total"`
	Limit        int               `json:"limit"`
	Offset       int               `json:"offset"`
}

// TransactionItem represents a single transaction in the response.
type TransactionItem struct {
	TxHash       string `json:"tx_hash"`
	Height       int64  `json:"height"`
	Timestamp    string `json:"timestamp"`
	UatomAmount  string `json:"uatom_amount"`
	AtomAmount   string `json:"atom_amount"`
	AtomPriceUSD string `json:"atom_price_usd"`
	USDValue     string `json:"usd_value"`
	Sender       string `json:"sender,omitempty"`
	Action       string `json:"action,omitempty"`
	Contract     string `json:"contract,omitempty"`
}

// handleTransactions returns recent burn transactions.
func (s *Server) handleTransactions(w http.ResponseWriter, r *http.Request) {
	limit := 100
	offset := 0

	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 1000 {
			limit = parsed
		}
	}
	if o := r.URL.Query().Get("offset"); o != "" {
		if parsed, err := strconv.Atoi(o); err == nil && parsed >= 0 {
			offset = parsed
		}
	}

	burns, total, err := db.GetRecentBurns(r.Context(), s.pool, limit, offset)
	if err != nil {
		s.logger.Error("failed to get burns", "error", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	resp := TransactionsResponse{
		Transactions: make([]TransactionItem, 0, len(burns)),
		Total:        total,
		Limit:        limit,
		Offset:       offset,
	}

	for _, b := range burns {
		atomAmount := b.UatomAmount.Div(decimal.NewFromInt(1_000_000))
		resp.Transactions = append(resp.Transactions, TransactionItem{
			TxHash:       b.TxHash,
			Height:       b.Height,
			Timestamp:    b.Timestamp.Format(time.RFC3339),
			UatomAmount:  b.UatomAmount.String(),
			AtomAmount:   atomAmount.StringFixed(6),
			AtomPriceUSD: b.AtomPriceUSD.StringFixed(6),
			USDValue:     b.USDValue.StringFixed(10),
			Sender:       b.Sender,
			Action:       b.Action,
			Contract:     b.Contract,
		})
	}

	s.writeJSON(w, resp)
}

// BreakdownItem represents one row in the action or contract breakdown.
type BreakdownItem struct {
	Label string `json:"label"`
	Txs   int64  `json:"txs"`
	Atom  string `json:"atom"`
}

// BreakdownResponse is the response for /api/breakdown.
type BreakdownResponse struct {
	ByAction   []BreakdownItem `json:"by_action"`
	ByContract []BreakdownItem `json:"by_contract"`
}

// handleBreakdown returns fee revenue grouped by action and contract.
func (s *Server) handleBreakdown(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	var resp BreakdownResponse

	// By action
	rows, err := s.pool.Query(ctx, `
		SELECT COALESCE(NULLIF(action, ''), 'unknown'), COUNT(*),
		       (SUM(uatom_amount)/1000000)::TEXT
		FROM roi_tracker.processed_burns
		GROUP BY action ORDER BY SUM(uatom_amount) DESC
	`)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var item BreakdownItem
			if err := rows.Scan(&item.Label, &item.Txs, &item.Atom); err == nil {
				resp.ByAction = append(resp.ByAction, item)
			}
		}
	}

	// By contract (with label from discovered_contracts)
	rows2, err := s.pool.Query(ctx, `
		SELECT COALESCE(dc.label, pb.contract), COUNT(*),
		       (SUM(pb.uatom_amount)/1000000)::TEXT
		FROM roi_tracker.processed_burns pb
		LEFT JOIN roi_tracker.discovered_contracts dc ON dc.address = pb.contract
		GROUP BY COALESCE(dc.label, pb.contract)
		ORDER BY SUM(pb.uatom_amount) DESC
	`)
	if err == nil {
		defer rows2.Close()
		for rows2.Next() {
			var item BreakdownItem
			if err := rows2.Scan(&item.Label, &item.Txs, &item.Atom); err == nil {
				resp.ByContract = append(resp.ByContract, item)
			}
		}
	}

	s.writeJSON(w, resp)
}

// HealthResponse is the response for the /health endpoint.
type HealthResponse struct {
	Status          string `json:"status"`
	LastProcessedID int64  `json:"last_processed_event_id"`
	ChainID         string `json:"chain_id"`
	ContractAddress string `json:"contract_address,omitempty"`
}

// handleHealth returns the service health status.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	stats, err := db.GetStats(r.Context(), s.pool)
	if err != nil {
		resp := HealthResponse{Status: "unhealthy"}
		w.WriteHeader(http.StatusServiceUnavailable)
		s.writeJSON(w, resp)
		return
	}

	resp := HealthResponse{
		Status:          "ok",
		LastProcessedID: stats.LastProcessedID,
		ChainID:         stats.ChainID,
		ContractAddress: stats.ContractAddress,
	}
	s.writeJSON(w, resp)
}

// writeJSON writes a JSON response.
func (s *Server) writeJSON(w http.ResponseWriter, data any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		s.logger.Error("failed to encode JSON", "error", err)
	}
}
