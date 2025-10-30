package btc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type BlockstreamProvider struct {
	BaseURL   string
	HTTP      *http.Client
	UserAgent string
}

type addressResp struct {
	ChainStats struct {
		FundedTxoSum uint64 `json:"funded_txo_sum"`
		SpentTxoSum  uint64 `json:"spent_txo_sum"`
	} `json:"chain_stats"`
	MempoolStats struct {
		FundedTxoSum uint64 `json:"funded_txo_sum"`
		SpentTxoSum  uint64 `json:"spent_txo_sum"`
	} `json:"mempool_stats"`
}

func NewBlockstreamProvider(baseURL, userAgent string, timeout time.Duration) *BlockstreamProvider {
	if !strings.HasPrefix(baseURL, "http") {
		baseURL = "https://blockstream.info/api"
	}
	return &BlockstreamProvider{
		BaseURL:   baseURL,
		HTTP:      &http.Client{Timeout: timeout},
		UserAgent: userAgent,
	}
}

func (p *BlockstreamProvider) Name() string { return "blockstream" }

func (p *BlockstreamProvider) GetBalance(ctx context.Context, address string, includeMempool bool) (Balance, error) {
	url := fmt.Sprintf("%s/address/%s", strings.TrimRight(p.BaseURL, "/"), address)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Balance{}, err
	}
	if p.UserAgent != "" {
		req.Header.Set("User-Agent", p.UserAgent)
	}
	resp, err := p.HTTP.Do(req)
	if err != nil {
		return Balance{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Balance{}, fmt.Errorf("blockstream: status %d", resp.StatusCode)
	}
	var ar addressResp
	if err := json.NewDecoder(resp.Body).Decode(&ar); err != nil {
		return Balance{}, err
	}
	confirmed := ar.ChainStats.FundedTxoSum - ar.ChainStats.SpentTxoSum
	if !includeMempool {
		return Balance{Address: address, Sats: confirmed}, nil
	}
	mempool := ar.MempoolStats.FundedTxoSum - ar.MempoolStats.SpentTxoSum
	return Balance{Address: address, Sats: confirmed + mempool}, nil
}
