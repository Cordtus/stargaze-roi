// Package price handles ATOM price fetching from CoinGecko.
package price

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/cordt-sei/starmos-roi-tracker/internal/db"
)

const (
	maxRetries     = 3
	baseRetryDelay = 2 * time.Second
)

// Fetcher handles price fetching from CoinGecko with rate limiting and caching.
type Fetcher struct {
	apiBase    string
	apiKey     string
	httpClient *http.Client
	pool       *pgxpool.Pool
	logger     *slog.Logger

	// Rate limiting
	mu          sync.Mutex
	lastRequest time.Time
	minInterval time.Duration
}

// NewFetcher creates a new price fetcher.
func NewFetcher(apiBase, apiKey string, rateLimitPerMin int, pool *pgxpool.Pool, logger *slog.Logger) *Fetcher {
	interval := time.Minute / time.Duration(rateLimitPerMin)

	return &Fetcher{
		apiBase:     apiBase,
		apiKey:      apiKey,
		pool:        pool,
		logger:      logger,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		minInterval: interval,
	}
}

// GetPrice returns the ATOM price in USD for the given timestamp.
// Uses caching at 1-minute granularity to minimize API calls.
func (f *Fetcher) GetPrice(ctx context.Context, timestamp time.Time) (decimal.Decimal, error) {
	// Round to minute for cache key
	minute := timestamp.UTC().Truncate(time.Minute)

	// Check cache first
	if price, found, err := db.GetCachedPrice(ctx, f.pool, minute); err != nil {
		f.logger.Warn("price cache lookup failed", "error", err, "minute", minute)
	} else if found {
		return price, nil
	}

	// Fetch from API
	price, err := f.fetchHistoricalPrice(ctx, timestamp)
	if err != nil {
		return decimal.Zero, err
	}

	// Cache the result
	if err := db.CachePrice(ctx, f.pool, minute, price); err != nil {
		f.logger.Warn("failed to cache price", "error", err, "minute", minute)
	}

	return price, nil
}

// GetCurrentPrice returns the current ATOM price in USD.
func (f *Fetcher) GetCurrentPrice(ctx context.Context) (decimal.Decimal, error) {
	return f.GetPrice(ctx, time.Now())
}

// errRateLimited signals a 429 response from CoinGecko.
var errRateLimited = errors.New("rate limited by CoinGecko")

// fetchHistoricalPrice fetches the ATOM price at a specific timestamp from CoinGecko.
func (f *Fetcher) fetchHistoricalPrice(ctx context.Context, timestamp time.Time) (decimal.Decimal, error) {
	// For recent timestamps (within last 24h), use simple price endpoint
	if time.Since(timestamp) < 24*time.Hour {
		return f.fetchCurrentPrice(ctx)
	}

	date := timestamp.UTC().Format("02-01-2006") // DD-MM-YYYY format for CoinGecko
	url := fmt.Sprintf("%s/coins/cosmos/history?date=%s&localization=false", f.apiBase, date)

	var lastErr error
	for attempt := range maxRetries {
		f.rateLimit()

		price, err := f.doHistoricalRequest(ctx, url, date)
		if err == nil {
			return price, nil
		}
		lastErr = err

		if !errors.Is(err, errRateLimited) && !isTransient(err) {
			return decimal.Zero, err
		}

		delay := backoffDelay(attempt)
		f.logger.Warn("retrying CoinGecko request", "attempt", attempt+1, "delay", delay, "error", err)
		select {
		case <-ctx.Done():
			return decimal.Zero, ctx.Err()
		case <-time.After(delay):
		}
	}

	return decimal.Zero, fmt.Errorf("exhausted retries: %w", lastErr)
}

// doHistoricalRequest performs a single historical price request.
func (f *Fetcher) doHistoricalRequest(ctx context.Context, url, date string) (decimal.Decimal, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return decimal.Zero, fmt.Errorf("creating request: %w", err)
	}
	f.addHeaders(req)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return decimal.Zero, fmt.Errorf("fetching historical price: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return decimal.Zero, errRateLimited
	}
	if resp.StatusCode >= 500 {
		return decimal.Zero, fmt.Errorf("CoinGecko server error: %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return decimal.Zero, fmt.Errorf("CoinGecko returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		MarketData struct {
			CurrentPrice struct {
				USD float64 `json:"usd"`
			} `json:"current_price"`
		} `json:"market_data"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return decimal.Zero, fmt.Errorf("decoding response: %w", err)
	}

	if result.MarketData.CurrentPrice.USD == 0 {
		return decimal.Zero, fmt.Errorf("no price data available for %s", date)
	}

	return decimal.NewFromFloat(result.MarketData.CurrentPrice.USD), nil
}

// fetchCurrentPrice fetches the current ATOM price with retry.
func (f *Fetcher) fetchCurrentPrice(ctx context.Context) (decimal.Decimal, error) {
	url := fmt.Sprintf("%s/simple/price?ids=cosmos&vs_currencies=usd&precision=full", f.apiBase)

	var lastErr error
	for attempt := range maxRetries {
		f.rateLimit()

		price, err := f.doCurrentRequest(ctx, url)
		if err == nil {
			return price, nil
		}
		lastErr = err

		if !errors.Is(err, errRateLimited) && !isTransient(err) {
			return decimal.Zero, err
		}

		delay := backoffDelay(attempt)
		f.logger.Warn("retrying CoinGecko request", "attempt", attempt+1, "delay", delay, "error", err)
		select {
		case <-ctx.Done():
			return decimal.Zero, ctx.Err()
		case <-time.After(delay):
		}
	}

	return decimal.Zero, fmt.Errorf("exhausted retries: %w", lastErr)
}

// doCurrentRequest performs a single current price request.
func (f *Fetcher) doCurrentRequest(ctx context.Context, url string) (decimal.Decimal, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return decimal.Zero, fmt.Errorf("creating request: %w", err)
	}
	f.addHeaders(req)

	resp, err := f.httpClient.Do(req)
	if err != nil {
		return decimal.Zero, fmt.Errorf("fetching current price: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return decimal.Zero, errRateLimited
	}
	if resp.StatusCode >= 500 {
		return decimal.Zero, fmt.Errorf("CoinGecko server error: %d", resp.StatusCode)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return decimal.Zero, fmt.Errorf("CoinGecko returned status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Cosmos struct {
			USD json.Number `json:"usd"`
		} `json:"cosmos"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return decimal.Zero, fmt.Errorf("decoding response: %w", err)
	}

	price, err := decimal.NewFromString(result.Cosmos.USD.String())
	if err != nil {
		return decimal.Zero, fmt.Errorf("parsing price: %w", err)
	}

	return price, nil
}

// backoffDelay returns an exponential backoff duration for the given attempt.
func backoffDelay(attempt int) time.Duration {
	return time.Duration(math.Pow(2, float64(attempt))) * baseRetryDelay
}

// isTransient returns true for errors likely to resolve on retry.
func isTransient(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, substr := range []string{"server error", "connection reset", "timeout", "EOF"} {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

// addHeaders adds required headers to the request.
func (f *Fetcher) addHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/json")
	if f.apiKey != "" {
		req.Header.Set("x-cg-demo-api-key", f.apiKey)
	}
}

// rateLimit blocks until it's safe to make another API request.
func (f *Fetcher) rateLimit() {
	f.mu.Lock()
	defer f.mu.Unlock()

	since := time.Since(f.lastRequest)
	if since < f.minInterval {
		time.Sleep(f.minInterval - since)
	}
	f.lastRequest = time.Now()
}

// Backfill populates the price cache for every day in the given range.
// Paced to stay within the CoinGecko rate limit. Skips dates already cached.
// Returns the number of prices fetched.
func (f *Fetcher) Backfill(ctx context.Context, from, to time.Time) (int, error) {
	from = from.UTC().Truncate(24 * time.Hour)
	to = to.UTC().Truncate(24 * time.Hour)

	fetched := 0
	for d := from; !d.After(to); d = d.Add(24 * time.Hour) {
		select {
		case <-ctx.Done():
			return fetched, ctx.Err()
		default:
		}

		minute := d.Truncate(time.Minute)

		// Skip if already cached
		if _, found, err := db.GetCachedPrice(ctx, f.pool, minute); err == nil && found {
			continue
		}

		price, err := f.fetchHistoricalPrice(ctx, d)
		if err != nil {
			f.logger.Warn("backfill: failed to fetch price, will retry later",
				"date", d.Format("2006-01-02"), "error", err)
			continue
		}

		if err := db.CachePrice(ctx, f.pool, minute, price); err != nil {
			f.logger.Warn("backfill: failed to cache price", "date", d.Format("2006-01-02"), "error", err)
			continue
		}

		fetched++
		f.logger.Info("backfill: cached price",
			"date", d.Format("2006-01-02"),
			"price", price.StringFixed(4),
			"progress", fmt.Sprintf("%d days", fetched),
		)
	}

	return fetched, nil
}

// UatomToAtom converts uatom (micro-ATOM) to ATOM.
// 1 ATOM = 1,000,000 uatom
func UatomToAtom(uatom decimal.Decimal) decimal.Decimal {
	return uatom.Div(decimal.NewFromInt(1_000_000))
}

// CalculateUSD calculates the USD value of uatom at the given price per ATOM.
func CalculateUSD(uatom decimal.Decimal, atomPriceUSD decimal.Decimal) decimal.Decimal {
	atom := UatomToAtom(uatom)
	return atom.Mul(atomPriceUSD)
}
