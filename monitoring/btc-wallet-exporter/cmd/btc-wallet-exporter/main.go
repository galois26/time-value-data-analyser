package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"btc-wallet-exporter/internal/btc"
	"btc-wallet-exporter/internal/config"
	"btc-wallet-exporter/internal/exporter"
	"btc-wallet-exporter/internal/price"
)

func main() {
	cfgPath := flag.String("config", "config.yml", "Path to YAML config file")
	scrapeEvery := flag.Duration("interval", 30*time.Second, "How often to refresh balances")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// Build BTC provider
	var btcProv btc.BalanceProvider
	switch cfg.Bitcoin.Provider.Type {
	case "blockstream", "":
		btcProv = btc.NewBlockstreamProvider(cfg.Bitcoin.Provider.BaseURL, cfg.Bitcoin.Provider.UserAgent, cfg.Bitcoin.Provider.Timeout)
	default:
		log.Fatalf("unsupported BTC provider: %s", cfg.Bitcoin.Provider.Type)
	}

	// after loading cfg (internal/config)
	var priceProv price.PriceProvider
	if cfg.Price.Enabled {
		p, err := price.NewProviderFromConfig(cfg.Price)
		if err != nil {
			log.Fatalf("price provider: %v", err)
		}
		priceProv = p
	}

	exp := exporter.NewExporter(
		cfg.Server.ListenAddress,
		cfg.Server.ReadTimeout,
		cfg.Server.WriteTimeout,
		cfg.Server.IdleTimeout,
		cfg.Bitcoin.Addresses,
		cfg.Bitcoin.IncludeMempool,
		btcProv,
		priceProv,
		cfg.Price.Currency,
		cfg.Price.CacheTTL,
	)

	// Periodic collection loop
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			if err := exp.Collect(ctx); err != nil {
				log.Printf("collect error: %v", err)
			}
			time.Sleep(*scrapeEvery)
		}
	}()

	// HTTP server
	go func() {
		log.Printf("serving /metrics on %s", cfg.Server.ListenAddress)
		if err := exp.Serve(); err != nil {
			log.Printf("http server: %v", err)
		}
	}()

	// Graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")
	shutdownCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	_ = exp.Shutdown(shutdownCtx)
}
