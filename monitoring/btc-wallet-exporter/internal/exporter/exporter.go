package exporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"btc-wallet-exporter/internal/btc"
	"btc-wallet-exporter/internal/config"
	"btc-wallet-exporter/internal/price"
)

type Exporter struct {
	Addresses      []string
	IncludeMempool bool
	BTC            btc.BalanceProvider
	Price          price.PriceProvider // optional (can be nil)
	PriceCurrency  string
	PriceTTL       time.Duration
	priceProvider  config.PriceProvider

	// internals
	mux    *http.ServeMux
	server *http.Server
	// metrics
	balanceSats   *prometheus.GaugeVec
	balanceBTC    *prometheus.GaugeVec
	balanceFiat   *prometheus.GaugeVec
	scrapeDur     prometheus.Summary
	reqTotal      *prometheus.CounterVec
	lastSuccessTS *prometheus.GaugeVec
	priceGauge    *prometheus.GaugeVec

	priceCacheUntil time.Time
	priceCacheVal   float64
	priceCacheCur   string
	mu              sync.RWMutex
}

func NewExporter(addr string, readTO, writeTO, idleTO time.Duration, addresses []string, includeMempool bool, btcProv btc.BalanceProvider, priceProv price.PriceProvider, priceCur string, priceTTL time.Duration) *Exporter {

	mux := http.NewServeMux()
	e := &Exporter{
		Addresses:      addresses,
		IncludeMempool: includeMempool,
		BTC:            btcProv,
		Price:          priceProv,
		PriceCurrency:  priceCur,
		PriceTTL:       priceTTL,
		mux:            mux,
	}
	// Register metrics
	e.balanceSats = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "btc",
		Name:      "wallet_balance_sats",
		Help:      "BTC wallet balance in satoshis",
	}, []string{"address"})
	e.balanceBTC = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "btc",
		Name:      "wallet_balance_btc",
		Help:      "BTC wallet balance in BTC",
	}, []string{"address"})
	e.balanceFiat = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "btc",
		Name:      "wallet_balance_fiat",
		Help:      "BTC wallet balance in configured fiat currency",
	}, []string{"address", "currency"})
	e.scrapeDur = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace: "btc_exporter",
		Name:      "scrape_duration_seconds",
		Help:      "Time spent scraping balances",
	})
	e.reqTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "btc_exporter",
		Name:      "requests_total",
		Help:      "Number of provider requests by status",
	}, []string{"provider", "status"})
	e.lastSuccessTS = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "btc_exporter",
		Name:      "last_success_timestamp_seconds",
		Help:      "Unix timestamp of the last successful full scrape",
	}, []string{"provider"})
	e.priceGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "btc",
		Name:      "price",
		Help:      "Price of 1 BTC in the configured fiat currency",
	}, []string{"currency"})
	prometheus.MustRegister(e.priceGauge)

	prometheus.MustRegister(
		e.balanceSats, e.balanceBTC, e.balanceFiat,
		e.scrapeDur, e.reqTotal, e.lastSuccessTS,
	)

	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	e.server = &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  readTO,
		WriteTimeout: writeTO,
		IdleTimeout:  idleTO,
	}
	return e
}

func (e *Exporter) Serve() error                       { return e.server.ListenAndServe() }
func (e *Exporter) Shutdown(ctx context.Context) error { return e.server.Shutdown(ctx) }

// Collect fetches balances concurrently and updates metrics.
func (e *Exporter) Collect(ctx context.Context) error {
	start := time.Now()
	defer func() { e.scrapeDur.Observe(time.Since(start).Seconds()) }()

	if e.BTC == nil {
		return errors.New("no BTC provider configured")
	}

	// Optional: get price (cached)
	var priceVal float64
	if e.Price != nil && e.PriceCurrency != "" {
		priceVal = e.getPriceCached(ctx)
	}
	if priceVal > 0 {
		e.priceGauge.WithLabelValues(strings.ToUpper(e.PriceCurrency)).Set(priceVal)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(e.Addresses))
	errCh := make(chan error, len(e.Addresses))

	for _, addr := range e.Addresses {
		address := addr
		go func() {
			defer wg.Done()
			c, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			bal, err := e.BTC.GetBalance(c, address, e.IncludeMempool)
			if err != nil {
				e.reqTotal.WithLabelValues(e.BTC.Name(), "error").Inc()
				errCh <- fmt.Errorf("address %s: %w", address, err)
				return
			}
			e.reqTotal.WithLabelValues(e.BTC.Name(), "ok").Inc()
			btcVal := float64(bal.Sats) / 1e8
			e.balanceSats.WithLabelValues(address).Set(float64(bal.Sats))
			e.balanceBTC.WithLabelValues(address).Set(btcVal)
			if priceVal > 0 {
				e.balanceFiat.WithLabelValues(address, strings.ToUpper(e.PriceCurrency)).Set(btcVal * priceVal)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	var gotErr error
	for err := range errCh {
		if gotErr == nil {
			gotErr = err
		} else {
			gotErr = fmt.Errorf("%v; %w", gotErr, err)
		}
	}
	if gotErr == nil {
		e.lastSuccessTS.WithLabelValues(e.BTC.Name()).Set(float64(time.Now().Unix()))
	}
	return gotErr
}

// getPriceCached reads or refreshes the cached price.
func (e *Exporter) getPriceCached(ctx context.Context) float64 {
	e.mu.RLock()
	if time.Now().Before(e.priceCacheUntil) && e.priceCacheCur == e.PriceCurrency && e.priceCacheVal > 0 {
		val := e.priceCacheVal
		e.mu.RUnlock()
		return val
	}
	e.mu.RUnlock()

	p, err := e.Price.GetBTCPrice(ctx, e.PriceCurrency)
	if err != nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.priceCacheVal = p.Value
	e.priceCacheCur = e.PriceCurrency
	e.priceCacheUntil = time.Now().Add(e.PriceTTL)
	return p.Value
}
