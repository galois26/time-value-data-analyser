package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/model"
	"time-value-analyser/multi-ingester/internal/util"
)

type lokiSink struct {
	cfg    config.LokiConfig
	client *http.Client
}

func NewLoki(cfg config.LokiConfig) Sink {
	to := cfg.Timeout
	if to == 0 {
		to = 10 * time.Second
	}
	return &lokiSink{cfg: cfg, client: util.NewHTTPClient(to)}
}

func (l *lokiSink) Name() string { return "loki" }

func (l *lokiSink) Push(ctx context.Context, events []model.Event) error {
	if len(events) == 0 {
		return nil
	}

	type stream struct {
		Stream map[string]string `json:"stream"`
		Values [][2]string       `json:"values"`
	}
	payload := struct {
		Streams []stream `json:"streams"`
	}{}
	for _, e := range events {
		line, _ := json.Marshal(map[string]any{
			"id":        e.ID,
			"title":     e.Title,
			"summary":   e.Summary,
			"url":       e.URL,
			"labels":    e.Labels,
			"published": e.Published.Format(time.RFC3339),
			"source":    e.Source,
		})
		// line: compact JSON (Raw + selected fields)
		lbls := map[string]string{
			"source": e.Source,
		}
		for k, v := range e.Labels {
			lbls[k] = v
		}

		// Loki expects ns timestamp as a decimal string
		ts := e.Published.UnixNano()
		payload.Streams = append(payload.Streams, stream{
			Stream: lbls,
			Values: [][2]string{
				{fmt.Sprintf("%d", ts), string(line)},
			},
		})
	}

	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, l.cfg.URL+"/loki/api/v1/push", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if l.cfg.TenantID != "" {
		req.Header.Set("X-Scope-OrgID", l.cfg.TenantID)
	}
	if ua := l.cfg.UserAgent; ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	resp, err := l.client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("loki push failed http %d", resp.StatusCode)
	}
	return nil
}
