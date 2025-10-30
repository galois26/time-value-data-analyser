package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Server struct {
	ListenAddress string        `yaml:"listen_address"`
	ReadTimeout   time.Duration `yaml:"read_timeout"`
	WriteTimeout  time.Duration `yaml:"write_timeout"`
	IdleTimeout   time.Duration `yaml:"idle_timeout"`
}

type BTCProvider struct {
	Type      string        `yaml:"type"`
	BaseURL   string        `yaml:"base_url"`
	Timeout   time.Duration `yaml:"timeout"`
	UserAgent string        `yaml:"user_agent"`
}

type Bitcoin struct {
	Addresses      []string    `yaml:"addresses"`
	IncludeMempool bool        `yaml:"include_mempool"`
	Provider       BTCProvider `yaml:"provider"`
}

type PriceProvider struct {
	Type      string        `yaml:"type"`
	BaseURL   string        `yaml:"base_url"`
	APIKey    string        `yaml:"api_key"` // NEW
	Timeout   time.Duration `yaml:"timeout"`
	UserAgent string        `yaml:"user_agent"`
}

type Price struct {
	Enabled  bool          `yaml:"enabled"`
	Currency string        `yaml:"currency"`
	CacheTTL time.Duration `yaml:"cache_ttl"`
	Provider PriceProvider `yaml:"provider"`
}

type Config struct {
	Server  Server  `yaml:"server"`
	Bitcoin Bitcoin `yaml:"bitcoin"`
	Price   Price   `yaml:"price"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	// Defaults
	if c.Server.ListenAddress == "" {
		c.Server.ListenAddress = ":9108"
	}
	if c.Server.ReadTimeout == 0 {
		c.Server.ReadTimeout = 5 * time.Second
	}
	if c.Server.WriteTimeout == 0 {
		c.Server.WriteTimeout = 5 * time.Second
	}
	if c.Server.IdleTimeout == 0 {
		c.Server.IdleTimeout = 60 * time.Second
	}
	if c.Bitcoin.Provider.Type == "" {
		c.Bitcoin.Provider.Type = "blockstream"
	}
	if c.Bitcoin.Provider.Timeout == 0 {
		c.Bitcoin.Provider.Timeout = 4 * time.Second
	}
	if c.Bitcoin.Provider.BaseURL == "" {
		c.Bitcoin.Provider.BaseURL = "https://blockstream.info/api"
	}
	if c.Price.Currency == "" {
		c.Price.Currency = "EUR"
	}
	if c.Price.Provider.Type == "" {
		c.Price.Provider.Type = "coingecko"
	}
	if c.Price.Provider.Timeout == 0 {
		c.Price.Provider.Timeout = 4 * time.Second
	}
	if c.Price.CacheTTL == 0 {
		c.Price.CacheTTL = 60 * time.Second
	}
	if c.Price.Provider.BaseURL == "" {
		c.Price.Provider.BaseURL = "https://api.coingecko.com/api/v3"
	}
	return &c, nil
}
