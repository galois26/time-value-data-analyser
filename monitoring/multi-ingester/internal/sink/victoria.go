package sink

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/model"
	"time-value-analyser/multi-ingester/internal/util"
)

type victoriaSink struct {
	cfg    config.VictoriaConfig
	client *http.Client
}

func NewVictoria(cfg config.VictoriaConfig) (Sink, error) {
	to := cfg.Timeout
	if to == 0 {
		to = 10 * time.Second
	}
	return &victoriaSink{
		cfg:    cfg,
		client: util.NewHTTPClient(to),
	}, nil
}

func (v *victoriaSink) Name() string { return "victoria" }

func (v *victoriaSink) Push(ctx context.Context, events []model.Event) error {
	if len(events) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, e := range events {
		ts := e.Published.Unix()
		metricName := "event_count_total"
		lbls := fmt.Sprintf(`source="%s"`, e.Source)
		for k, val := range e.Labels {
			lbls += fmt.Sprintf(`,%s="%s"`, k, val)
		}
		line := fmt.Sprintf(`%s{%s} 1 %d\n`, metricName, lbls, ts*1000)
		buf.WriteString(line)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, v.cfg.URL+"/api/v1/import/prometheus", &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain")
	if ua := v.cfg.UserAgent; ua != "" {
		req.Header.Set("User-Agent", ua)
	}

	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("victoria push failed: %s", resp.Status)
	}
	return nil
}

// Emit simple counters per label group for this batch at ingest time.
// metric names: news_events_count{source,language,env,...}
/*
func (v *Victoria) EmitCounts(ctx context.Context, metric string, baseLabels map[string]string, events []model.Event) error {
	if v.url == "" || len(events) == 0 {
		return nil
	}
	ts := time.Now().UnixMilli()
	type key struct{ k string }
	groups := map[key]int{}
	for _, ev := range events {
		lbls := map[string]string{}
		for k, v := range baseLabels {
			lbls[k] = v
		}
		lbls["source"] = ev.Source
		if ev.Lang != "" {
			lbls["language"] = ev.Lang
		}
		if ev.Country != "" {
			lbls["country"] = ev.Country
		}
		for k, v := range ev.Labels {
			lbls[k] = v
		}
		// deterministic label ordering for cache friendliness
		keys := make([]string, 0, len(lbls))
		for k := range lbls {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b := bytes.Buffer{}
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			fmt.Fprintf(&b, "%s=\"%s\"", k, escape(lbls[k]))
		}
		groups[key{"{" + b.String() + "}"}]++
	}
	// build Prom text
	buf := &bytes.Buffer{}
	for k, n := range groups {
		fmt.Fprintf(buf, "%s%s %d %d\n", metric, k.k, n, ts)
	}
	// POST
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, v.url+"/api/v1/import/prometheus", bytes.NewReader(buf.Bytes()))
	req.Header.Set("Content-Type", "text/plain")
	resp, err := v.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("victoria http %d", resp.StatusCode)
	}
	return nil
}

func escape(s string) string {
	// minimal escape for label values
	res := make([]rune, 0, len(s))
	for _, r := range s {
		if r == '"' {
			res = append(res, '\\', '"')
		} else if r == '\\' {
			res = append(res, '\\', '\\')
		} else {
			res = append(res, r)
		}
	}
	return string(res)
}
*/
