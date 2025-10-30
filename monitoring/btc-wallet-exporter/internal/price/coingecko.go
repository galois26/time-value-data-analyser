package price

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// CoinGecko docs: https://docs.coingecko.com/
// Auth header: "x-cg-pro-api-key: <KEY>" (works for free & pro keys)
// Endpoint used: /simple/price?ids=bitcoin&vs_currencies=<fiat>

type CoinGecko struct {
	baseURL   string
	apiKey    string // optional
	userAgent string
	client    *http.Client
}

type cgResp map[string]map[string]float64

func NewCoinGecko(baseURL, apiKey, userAgent string, timeout time.Duration) *CoinGecko {
	if baseURL == "" {
		baseURL = "https://api.coingecko.com/api/v3"
	}
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 60 * time.Second,
		}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	return &CoinGecko{
		baseURL:   strings.TrimRight(baseURL, "/"),
		apiKey:    strings.TrimSpace(apiKey),
		userAgent: userAgent,
		client: &http.Client{
			Timeout:   timeout,
			Transport: tr,
		},
	}
}

func (c *CoinGecko) Name() string { return "coingecko" }

func (c *CoinGecko) GetBTCPrice(ctx context.Context, fiat string) (Price, error) {
	fiat = strings.ToLower(strings.TrimSpace(fiat))
	if fiat == "" {
		fiat = "usd"
	}
	q := url.Values{}
	q.Set("ids", "bitcoin")
	q.Set("vs_currencies", fiat)

	u := fmt.Sprintf("%s/simple/price?%s", c.baseURL, q.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return Price{}, err
	}
	if c.userAgent != "" {
		req.Header.Set("User-Agent", c.userAgent)
	}
	// Optional API key
	if c.apiKey != "" {
		req.Header.Set("x-cg-pro-api-key", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return Price{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return Price{}, fmt.Errorf("coingecko: rate limited (%d)", resp.StatusCode)
	}
	if resp.StatusCode/100 != 2 {
		return Price{}, fmt.Errorf("coingecko: http %d", resp.StatusCode)
	}

	var data cgResp
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return Price{}, err
	}
	m, ok := data["bitcoin"]
	if !ok {
		return Price{}, errors.New("coingecko: missing 'bitcoin' key")
	}
	val, ok := m[fiat]
	if !ok {
		return Price{}, fmt.Errorf("coingecko: missing fiat '%s'", fiat)
	}
	return Price{Value: val}, nil
}
