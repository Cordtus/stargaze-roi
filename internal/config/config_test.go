package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

const testConfig = `
[chain]
chain_id = "cosmoshub-4"
grpc_endpoint = "grpc.example.com:443"

[contract]
addresses = ["cosmos1abc", "cosmos1def"]

[coingecko]
api_base = "https://api.coingecko.com/api/v3"
rate_limit_per_minute = 10

[target]
usd_amount = "1500000.00"
grant_date = "2025-11-21"
proposal_id = 1017
grant_uatom_amount = "699626000000"
multisig_address = "cosmos1vdqfavw0cu0fpvlcl52ku3qztt38szktlpfsuz"

[server]
port = 8080
static_dir = "./static"

[database]
conn_string = "postgres://localhost:5432/cosmoflow?sslmode=disable"
`

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(testConfig), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Chain.ChainID != "cosmoshub-4" {
		t.Errorf("ChainID = %q, want cosmoshub-4", cfg.Chain.ChainID)
	}
	if cfg.Target.ProposalID != 1017 {
		t.Errorf("ProposalID = %d, want 1017", cfg.Target.ProposalID)
	}
	if cfg.Target.MultisigAddress != "cosmos1vdqfavw0cu0fpvlcl52ku3qztt38szktlpfsuz" {
		t.Errorf("MultisigAddress = %q", cfg.Target.MultisigAddress)
	}
	if len(cfg.Contract.Addresses) != 2 {
		t.Errorf("Addresses count = %d, want 2", len(cfg.Contract.Addresses))
	}
}

func TestTargetUSD(t *testing.T) {
	cfg := &Config{Target: TargetConfig{USDAmount: "1500000.00"}}
	want := decimal.NewFromFloat(1500000.00)
	if !cfg.TargetUSD().Equal(want) {
		t.Errorf("TargetUSD() = %s, want %s", cfg.TargetUSD(), want)
	}
}

func TestGrantDateParsed(t *testing.T) {
	cfg := &Config{Target: TargetConfig{GrantDate: "2025-11-21"}}
	got := cfg.GrantDateParsed()
	want := time.Date(2025, 11, 21, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("GrantDateParsed() = %v, want %v", got, want)
	}
}

func TestGrantDateParsedInvalid(t *testing.T) {
	cfg := &Config{Target: TargetConfig{GrantDate: "invalid"}}
	got := cfg.GrantDateParsed()
	if !got.IsZero() {
		t.Errorf("GrantDateParsed() = %v, want zero", got)
	}
}

func TestGrantAtom(t *testing.T) {
	cfg := &Config{Target: TargetConfig{GrantUatomAmount: "699626000000"}}
	want := decimal.NewFromFloat(699626.0)
	got := cfg.GrantAtom()
	if !got.Equal(want) {
		t.Errorf("GrantAtom() = %s, want %s", got, want)
	}
}

func TestEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if err := os.WriteFile(path, []byte(testConfig), 0644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("DATABASE_URL", "postgres://override:5432/test")
	t.Setenv("CONTRACT_ADDRESS", "cosmos1abc")
	t.Setenv("COINGECKO_API_KEY", "test-key")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load() error: %v", err)
	}

	if cfg.Database.ConnString != "postgres://override:5432/test" {
		t.Errorf("DATABASE_URL override failed: %q", cfg.Database.ConnString)
	}
	if len(cfg.Contract.Addresses) != 1 || cfg.Contract.Addresses[0] != "cosmos1abc" {
		t.Errorf("CONTRACT_ADDRESS override failed: %v", cfg.Contract.Addresses)
	}
	if cfg.CoinGecko.HistoricalAPIKey != "test-key" {
		t.Errorf("COINGECKO_API_KEY override failed: %q", cfg.CoinGecko.HistoricalAPIKey)
	}
}

func TestValidationMissingChainID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	content := `
[chain]
chain_id = ""
grpc_endpoint = "grpc.example.com:443"
[target]
usd_amount = "100"
[database]
conn_string = "postgres://localhost/db"
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := Load(path)
	if err == nil {
		t.Error("expected validation error for missing chain_id")
	}
}
