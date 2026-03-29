package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cordt-sei/starmos-roi-tracker/internal/config"
)

func TestWriteJSON(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	s := &Server{logger: logger}

	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}
	s.writeJSON(w, data)

	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var result map[string]string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("result = %v, want {key: value}", result)
	}
}

func TestCORSHeaders(t *testing.T) {
	cfg := &config.Config{
		Chain:  config.ChainConfig{ChainID: "cosmoshub-4"},
		Server: config.ServerConfig{Port: 9999, StaticDir: "."},
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := New(cfg, nil, logger, nil)

	req := httptest.NewRequest("OPTIONS", "/api/stats", nil)
	w := httptest.NewRecorder()
	srv.server.Handler.ServeHTTP(w, req)

	if origin := w.Header().Get("Access-Control-Allow-Origin"); origin != "*" {
		t.Errorf("CORS origin = %q, want *", origin)
	}
	if w.Code != http.StatusOK {
		t.Errorf("OPTIONS status = %d, want 200", w.Code)
	}
}

func TestStatsResponseJSON(t *testing.T) {
	years := 42.5
	resp := StatsResponse{
		TargetUSD:        "1500000.00",
		TotalFeesUSDHist: "100.00",
		TotalFeesAtom:    "10.000000",
		TransactionCount: 5,
		ProgressPercent:  "0.0066666667",
		YearsToBreakeven: &years,
		ChainID:          "cosmoshub-4",
		ProposalID:       1017,
		GrantAtom:        "699626.000000",
		Revenue: RevenueBreakdown{
			GasFees:      "9.500000",
			ProtocolFees: "0.300000",
			ListingFees:  "0.100000",
			CreationFees: "0.100000",
			Total:        "10.000000",
		},
	}

	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}

	if decoded["proposal_id"].(float64) != 1017 {
		t.Errorf("proposal_id = %v, want 1017", decoded["proposal_id"])
	}
	if decoded["years_to_breakeven"].(float64) != 42.5 {
		t.Errorf("years_to_breakeven = %v, want 42.5", decoded["years_to_breakeven"])
	}
	if decoded["grant_atom"].(string) != "699626.000000" {
		t.Errorf("grant_atom = %v, want 699626.000000", decoded["grant_atom"])
	}
}
