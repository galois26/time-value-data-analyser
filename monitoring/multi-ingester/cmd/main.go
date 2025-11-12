package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/metrics"
	"time-value-analyser/multi-ingester/internal/model"
	"time-value-analyser/multi-ingester/internal/postprocess"
	"time-value-analyser/multi-ingester/internal/sink"
	"time-value-analyser/multi-ingester/internal/source"
	"time-value-analyser/multi-ingester/internal/store"
)

// Version is set at build time via -ldflags "-X main.Version=..."
var Version = "dev"

func main() {
	var (
		cfgPath  = flag.String("config", "/config.yml", "path to YAML config")
		interval = flag.Duration("interval", 15*time.Minute, "run interval")
		once     = flag.Bool("once", false, "run a single cycle then exit")
		verbose  = flag.Bool("verbose", true, "enable verbose logging")
	)
	flag.Parse()

	log.Printf("multi-ingester %s starting...", Version)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}
	// Build sinks
	var sinks []sink.Sink
	if strings.TrimSpace(cfg.Loki.URL) != "" {
		s := sink.NewLoki(cfg.Loki)
		if err != nil {
			log.Fatalf("init loki sink: %v", err)
		}
		sinks = append(sinks, s)
	}
	if strings.TrimSpace(cfg.Victoria.URL) != "" {
		s, err := sink.NewVictoria(cfg.Victoria)
		if err != nil {
			log.Fatalf("init victoria sink: %v", err)
		}
		sinks = append(sinks, s)
	}
	if len(sinks) == 0 {
		log.Fatal("no sinks configured (need loki and/or victoria)")
	}

	// Dedup store (in-memory)
	var d *store.Dedup
	if cfg.Dedup.Enable {
		d = store.NewDedup(cfg.Dedup.MaxKeys, cfg.Dedup.TTL)
		log.Printf("dedup enabled: max=%d ttl=%s", cfg.Dedup.MaxKeys, cfg.Dedup.TTL)
	} else {
		log.Printf("dedup disabled")
	}

	// Build sources
	srcs := make([]source.Source, 0, len(cfg.Sources))
	for _, sc := range cfg.Sources {
		s, err := source.NewFromConfig(sc)
		if err != nil {
			log.Fatalf("build source %q: %v", sc.Type, err)
		}
		srcs = append(srcs, s)
		log.Printf("configured source: %s", s.Name())
	}
	if len(srcs) == 0 {
		log.Fatal("no sources configured")
	}
	// Main loop
	// Context with signal cancel
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	runOnce := func() {
		start := time.Now()
		total := 0

		for _, src := range srcs {
			// Fetch
			evs, err := src.Fetch(ctx)
			log.Printf("DEBUG MAIN: fetched %d events from %s", len(evs), src.Name())
			if err != nil {
				log.Printf("fetch %s: %v", src.Name(), err)
				continue
			}

			// Apply dedup (if enabled)
			if d != nil {
				before := len(evs)
				out := make([]model.Event, 0, len(evs))
				for _, e := range evs {
					key := e.Source + "::" + e.ID
					if d.Seen(key) {
						continue
					}
					out = append(out, e)
					// Mark temp to avoid duplicates within this batch; we'll extend TTL after successful sink push
					d.Mark(key)
				}
				if *verbose {
					log.Printf("%s: dedup filtered %d -> %d", src.Name(), before, len(out))
				}
				evs = out
			}

			if len(evs) == 0 {
				log.Printf("%s: no new events", src.Name())
				continue
			}

			// Post-process labels
			if *verbose {
				log.Printf("%s: applying postprocess rules", src.Name())
			}
			evs = postprocess.Apply(evs, cfg.Post)

			// Fan-out to all sinks
			var wg sync.WaitGroup
			errCh := make(chan error, len(sinks))
			for _, sk := range sinks {
				sk := sk
				wg.Add(1)
				go func() {
					defer wg.Done()
					if err := sk.Push(ctx, evs); err != nil {
						errCh <- fmt.Errorf("push %s -> %s: %w", src.Name(), sk.Name(), err)
					}
				}()
			}
			wg.Wait()
			close(errCh)
			hadErr := false
			for e := range errCh {
				hadErr = true
				log.Println(e)
			}
			if hadErr {
				// On sink error, we don't advance counters/state. Next cycle will retry.
				continue
			}

			// Success path: count + update metrics
			total += len(evs)
			metrics.IncCounter("events_pushed_total", map[string]string{"source": src.Name()}, float64(len(evs)))
			log.Printf("%s: pushed %d events to %d sink(s)", src.Name(), len(evs), len(sinks))
		}

		// Optional metrics snapshot
		if cfg.Metrics.Enable {
			snap := metrics.Dump()
			if snap != "" {
				fmt.Println("METRICS SNAPSHOT:" + snap)
			}
		}

		if *verbose {
			log.Printf("cycle finished in %s, total events=%d", time.Since(start).Truncate(time.Millisecond), total)
		}
	}

	// Run mode
	log.Printf("multi-ingester started: %d source(s), interval=%s", len(srcs), interval.String())
	runOnce()
	if *once {
		return
	}

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("stopping: %v", ctx.Err())
			return
		case <-ticker.C:
			runOnce()
		}
	}
}
