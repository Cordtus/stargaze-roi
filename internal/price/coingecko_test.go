package price

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestBackoffDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * baseRetryDelay},
		{1, 2 * baseRetryDelay},
		{2, 4 * baseRetryDelay},
	}
	for _, tt := range tests {
		got := backoffDelay(tt.attempt)
		if got != tt.want {
			t.Errorf("backoffDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestIsTransient(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errRateLimited, false},
		{http.ErrHandlerTimeout, true},
		{&transientErr{"connection reset by peer"}, true},
		{&transientErr{"server error: 503"}, true},
		{&transientErr{"unexpected EOF"}, true},
		{&transientErr{"no price data available"}, false},
	}
	for _, tt := range tests {
		got := isTransient(tt.err)
		if got != tt.want {
			t.Errorf("isTransient(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

type transientErr struct{ msg string }

func (e *transientErr) Error() string { return e.msg }

func TestFetchCurrentPrice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/simple/price" {
			http.Error(w, "not found", 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"cosmos": map[string]any{
				"usd": json.Number("7.42"),
			},
		})
	}))
	defer server.Close()

	f := NewFetcher(server.URL, "", 60, nil, noopLogger())

	price, err := f.fetchCurrentPrice(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := decimal.NewFromFloat(7.42)
	if !price.Equal(expected) {
		t.Errorf("got price %s, want %s", price, expected)
	}
}

func TestFetchCurrentPriceRetries429(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"cosmos": map[string]any{"usd": json.Number("8.00")},
		})
	}))
	defer server.Close()

	f := NewFetcher(server.URL, "", 600, nil, noopLogger())

	price, err := f.fetchCurrentPrice(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if !price.Equal(decimal.NewFromFloat(8.00)) {
		t.Errorf("got price %s, want 8.00", price)
	}
}

func TestFetchHistoricalPrice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/coins/cosmos/history" {
			http.Error(w, "not found", 404)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"market_data": map[string]any{
				"current_price": map[string]any{
					"usd": 6.50,
				},
			},
		})
	}))
	defer server.Close()

	f := NewFetcher(server.URL, "", 60, nil, noopLogger())

	// Use a timestamp older than 24h to trigger historical path
	ts := time.Now().Add(-48 * time.Hour)
	price, err := f.fetchHistoricalPrice(context.Background(), ts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := decimal.NewFromFloat(6.50)
	if !price.Equal(expected) {
		t.Errorf("got price %s, want %s", price, expected)
	}
}

func TestUatomToAtom(t *testing.T) {
	uatom := decimal.NewFromInt(1_500_000)
	atom := UatomToAtom(uatom)
	if !atom.Equal(decimal.NewFromFloat(1.5)) {
		t.Errorf("UatomToAtom(1500000) = %s, want 1.5", atom)
	}
}

func TestCalculateUSD(t *testing.T) {
	uatom := decimal.NewFromInt(2_000_000) // 2 ATOM
	price := decimal.NewFromFloat(7.50)
	usd := CalculateUSD(uatom, price)
	expected := decimal.NewFromFloat(15.00)
	if !usd.Equal(expected) {
		t.Errorf("CalculateUSD(2M, 7.50) = %s, want %s", usd, expected)
	}
}

func noopLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
