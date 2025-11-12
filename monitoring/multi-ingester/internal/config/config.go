package config

import (
	"errors"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type LokiConfig struct {
	URL       string        `yaml:"url"`       // http://loki:3100
	TenantID  string        `yaml:"tenant_id"` // optional multi-tenancy
	Job       string        `yaml:"job"`       // label value, default: multi-ingester
	Timeout   time.Duration `yaml:"timeout"`   // request timeout
	UserAgent string        `yaml:"user_agent"`
}

type VictoriaConfig struct {
	URL       string        `yaml:"url"`     // http://victoria-metrics:8428
	Timeout   time.Duration `yaml:"timeout"` // request timeout
	UserAgent string        `yaml:"user_agent"`
}

type CommonHTTP struct {
	Timeout     time.Duration `yaml:"timeout"`
	UserAgent   string        `yaml:"user_agent"`
	ContentType string        `yaml:"content_type"`
}

type GTAConfig struct {
	BaseURL string     `yaml:"base_url"` // e.g. https://api.globaltradealert.org
	APIKey  string     `yaml:"api_key"`  // if required; empty if public
	HTTP    CommonHTTP `yaml:"http"`
	// Query parameters (keep simple to start)
	Since      string `yaml:"since"`       // ISO date/time or relative token handled externally
	Country    string `yaml:"country"`     // optional filter
	DateFilter string `yaml:"date_filter"` // announcement_period | update_period | submission_period | in_force_on_date
	// Incremental sync controls
	Window    time.Duration `yaml:"window"`     // rolling window (e.g., 168h)
	StatePath string        `yaml:"state_path"` // persisted cursor file path
	// Resilience
	// Per-source rate limiting & retries
	RatePerSecond float64       `yaml:"rate_per_second"` // e.g. 1.0 = 1 req/sec
	Burst         int           `yaml:"burst"`           // token bucket burst (e.g. 2)
	MaxRetries    int           `yaml:"max_retries"`     // retry attempts (e.g. 3)
	Backoff       time.Duration `yaml:"backoff"`         // initial backoff (e.g. 500ms)
	MaxBackoff    time.Duration `yaml:"max_backoff"`     // cap (e.g. 5s)
}

type CoinDeskConfig struct {
	BaseURL    string        `yaml:"base_url"` // e.g. https://api.coindesk.com
	APIKey     string        `yaml:"api_key"`  // optional; set both Authorization: Bearer + X-API-KEY
	HTTP       CommonHTTP    `yaml:"http"`
	Topics     []string      `yaml:"topics"` // ["bitcoin","regulation"]
	Tags       []string      `yaml:"tags"`
	Language   string        `yaml:"language"`   // "EN"
	Window     time.Duration `yaml:"window"`     // rolling window, e.g. 24h
	StatePath  string        `yaml:"state_path"` // /data/coindesk-state.json
	PageSize   int           `yaml:"page_size"`  // default 100
	MaxPages   int           `yaml:"max_pages"`  // default 3
	MaxRetries int           `yaml:"max_retries"`
	Backoff    time.Duration `yaml:"backoff"`
	MaxBackoff time.Duration `yaml:"max_backoff"`
}

type SourceConfig struct {
	Type     string         `yaml:"type"` // "gta"
	GTA      GTAConfig      `yaml:"gta"`
	CoinDesk CoinDeskConfig `yaml:"coindesk"`
}

type KeywordRule struct {
	When   []string          `yaml:"when"`   // list of substrings (case-insensitive) to match in title/summary
	Labels map[string]string `yaml:"labels"` // labels to add when matched
}

type RegexRule struct {
	Field  string            `yaml:"field"` // title|summary|url
	Expr   string            `yaml:"expr"`
	Labels map[string]string `yaml:"labels"`
}

type MapRule struct {
	Field   string            `yaml:"field"`   // e.g. country
	Mapping map[string]string `yaml:"mapping"` // e.g. "DE":"EU"
	OutKey  string            `yaml:"out_key"` // label key to write, e.g. region
}

type PostProcessConfig struct {
	Keywords []KeywordRule `yaml:"keywords"`
	Regex    []RegexRule   `yaml:"regex"`
	Maps     []MapRule     `yaml:"maps"`
}

type MetricsConfig struct {
	Enable bool          `yaml:"enable"`
	Rollup time.Duration `yaml:"rollup"` // emit counts every run with current timestamp
}

type DedupConfig struct {
	Enable  bool          `yaml:"enable"`
	TTL     time.Duration `yaml:"ttl"`      // e.g. 168h (7d)
	MaxKeys int           `yaml:"max_keys"` // cap to bound memory
}

type Config struct {
	Loki     LokiConfig        `yaml:"loki"`
	Victoria VictoriaConfig    `yaml:"victoria"`
	Sources  []SourceConfig    `yaml:"sources"`
	Post     PostProcessConfig `yaml:"postprocess"`
	Metrics  MetricsConfig     `yaml:"metrics"`
	Dedup    DedupConfig       `yaml:"dedup"`
}

func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return Config{}, err
	}
	if c.Loki.URL == "" && c.Victoria.URL == "" {
		return c, errors.New("need at least one sink (loki or victoria)")
	}
	return c, nil
}
