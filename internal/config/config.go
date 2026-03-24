// Package config handles configuration loading and validation for the tracker.
package config

import (
	"fmt"
	"os"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/shopspring/decimal"
)

// Config represents the complete application configuration.
type Config struct {
	Chain     ChainConfig     `toml:"chain"`
	Contract  ContractConfig  `toml:"contract"`
	CoinGecko CoinGeckoConfig `toml:"coingecko"`
	Target    TargetConfig    `toml:"target"`
	Server    ServerConfig    `toml:"server"`
	Database  DatabaseConfig  `toml:"database"`
}

// ChainConfig holds Cosmos chain settings.
type ChainConfig struct {
	ChainID      string `toml:"chain_id"`
	GRPCEndpoint string `toml:"grpc_endpoint"`
}

// ContractConfig holds the target contract addresses to monitor.
type ContractConfig struct {
	// Addresses is the list of CosmWasm contract addresses to monitor
	Addresses []string `toml:"addresses"`
	// BurnAttribute is the event attribute key containing the burn amount
	BurnAttribute string `toml:"burn_attribute"`
	// BurnAction is the wasm action that indicates a burn (e.g., "burn", "burn_tokens")
	BurnAction string `toml:"burn_action"`
}

// CoinGeckoConfig holds price API settings.
type CoinGeckoConfig struct {
	APIBase          string `toml:"api_base"`
	RateLimitPerMin  int    `toml:"rate_limit_per_minute"`
	CacheMinutes     int    `toml:"cache_minutes"`
	HistoricalAPIKey string `toml:"api_key"`
}

// TargetConfig holds the ROI target settings.
type TargetConfig struct {
	// USDAmount is the grant amount in USD (e.g., "1500000.00")
	USDAmount string `toml:"usd_amount"`
	// GrantDate is when the grant was approved (YYYY-MM-DD)
	GrantDate string `toml:"grant_date"`
	// ProposalID is the governance proposal number
	ProposalID int `toml:"proposal_id"`
	// GrantUatomAmount is the ATOM grant amount in uatom
	GrantUatomAmount string `toml:"grant_uatom_amount"`
	// MultisigAddress is the fund management multisig
	MultisigAddress string `toml:"multisig_address"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port      int    `toml:"port"`
	StaticDir string `toml:"static_dir"`
}

// DatabaseConfig holds PostgreSQL connection settings.
type DatabaseConfig struct {
	// ConnString is the PostgreSQL connection string
	// e.g., "postgres://user:pass@localhost:5432/dbname"
	ConnString string `toml:"conn_string"`
}

// Load reads and parses the configuration file from the given path.
// Returns an error if the file cannot be read or is invalid.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing config file: %w", err)
	}

	// Override with environment variables if set
	cfg.applyEnvOverrides()

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}

	cfg.setDefaults()
	return &cfg, nil
}

// applyEnvOverrides applies environment variable overrides to config.
func (c *Config) applyEnvOverrides() {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		c.Database.ConnString = v
	}
	if v := os.Getenv("CONTRACT_ADDRESS"); v != "" {
		c.Contract.Addresses = []string{v}
	}
	if v := os.Getenv("COINGECKO_API_KEY"); v != "" {
		c.CoinGecko.HistoricalAPIKey = v
	}
}

// validate checks that required configuration values are present and valid.
func (c *Config) validate() error {
	if c.Chain.ChainID == "" {
		return fmt.Errorf("chain.chain_id is required")
	}
	if c.Chain.GRPCEndpoint == "" {
		return fmt.Errorf("chain.grpc_endpoint is required")
	}
	if c.Target.USDAmount == "" {
		return fmt.Errorf("target.usd_amount is required")
	}
	if _, err := decimal.NewFromString(c.Target.USDAmount); err != nil {
		return fmt.Errorf("target.usd_amount is not a valid decimal: %w", err)
	}
	if c.Database.ConnString == "" {
		return fmt.Errorf("database.conn_string is required")
	}
	return nil
}

// setDefaults fills in default values for optional configuration.
func (c *Config) setDefaults() {
	if c.CoinGecko.APIBase == "" {
		c.CoinGecko.APIBase = "https://api.coingecko.com/api/v3"
	}
	if c.CoinGecko.RateLimitPerMin == 0 {
		c.CoinGecko.RateLimitPerMin = 10
	}
	if c.CoinGecko.CacheMinutes == 0 {
		c.CoinGecko.CacheMinutes = 1
	}
	if c.Server.Port == 0 {
		c.Server.Port = 8080
	}
	if c.Server.StaticDir == "" {
		c.Server.StaticDir = "./static"
	}
	if c.Contract.BurnAttribute == "" {
		c.Contract.BurnAttribute = "amount"
	}
	if c.Contract.BurnAction == "" {
		c.Contract.BurnAction = "burn"
	}
}

// TargetUSD returns the target amount as a decimal for precise calculations.
func (c *Config) TargetUSD() decimal.Decimal {
	d, _ := decimal.NewFromString(c.Target.USDAmount)
	return d
}

// GrantDateParsed returns the grant date as a time.Time, or zero if unparseable.
func (c *Config) GrantDateParsed() time.Time {
	t, _ := time.Parse("2006-01-02", c.Target.GrantDate)
	return t
}

// GrantAtom returns the grant amount in ATOM (from uatom string) as a decimal.
func (c *Config) GrantAtom() decimal.Decimal {
	d, _ := decimal.NewFromString(c.Target.GrantUatomAmount)
	return d.Div(decimal.NewFromInt(1_000_000))
}
