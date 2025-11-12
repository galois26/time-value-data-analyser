package source

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/model"
	"time-value-analyser/multi-ingester/internal/store"
	"time-value-analyser/multi-ingester/internal/util"
)

type coinDeskSource struct {
	cfg    config.CoinDeskConfig
	client *http.Client
}

func NewCoinDeskSource(cfg config.CoinDeskConfig) *coinDeskSource {
	to := cfg.HTTP.Timeout
	if to == 0 {
		to = 15 * time.Second
	}
	return &coinDeskSource{cfg: cfg, client: util.NewHTTPClient(to)}
}

func (s *coinDeskSource) Name() string { return "coindesk" }

func (s *coinDeskSource) Fetch(ctx context.Context) ([]model.Event, error) {
	base := strings.TrimRight(s.cfg.BaseURL, "/")
	if base == "" {
		base = "https://data-api.coindesk.com"
	}
	endpoint := base + "/news/v1/article/list?lang=EN&limit=10" // generic list endpoint; adjust if your plan uses another path

	// Time window: rolling > state > last 24h
	var from, toStr string
	if s.cfg.Window > 0 {
		from = time.Now().UTC().Add(-s.cfg.Window).Format(time.RFC3339)
		toStr = time.Now().UTC().Format(time.RFC3339)
	} else if st, err := store.LoadNewsState(s.cfg.StatePath); err == nil && st.LastPublished != "" {
		from = st.LastPublished
		toStr = time.Now().UTC().Format(time.RFC3339)
	} else {
		from = time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
		toStr = time.Now().UTC().Format(time.RFC3339)
	}

	pageSize := s.cfg.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}
	maxPages := s.cfg.MaxPages
	if maxPages <= 0 {
		maxPages = 3
	}

	qTopics := strings.Join(s.cfg.Topics, ",")
	qTags := strings.Join(s.cfg.Tags, ",")

	var all []model.Event
	latest := from

	for page := 1; page <= maxPages; page++ {
		u, _ := url.Parse(endpoint)
		q := u.Query()
		q.Set("page", fmt.Sprintf("%d", page))
		q.Set("page_size", fmt.Sprintf("%d", pageSize))
		if qTopics != "" {
			q.Set("topics", qTopics)
		}
		if qTags != "" {
			q.Set("tags", qTags)
		}
		if s.cfg.Language != "" {
			q.Set("lang", s.cfg.Language)
		}
		q.Set("from", from)
		q.Set("to", toStr)
		u.RawQuery = q.Encode()

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		if ua := s.cfg.HTTP.UserAgent; ua != "" {
			req.Header.Set("User-Agent", ua)
		}
		if k := strings.TrimSpace(s.cfg.APIKey); k != "" {
			req.Header.Set("Authorization", "Bearer "+k)
			req.Header.Set("X-API-KEY", k)
		}

		var resp *http.Response
		err = util.Retry(ctx, max(1, s.cfg.MaxRetries), defaultDur(s.cfg.Backoff, 500*time.Millisecond), defaultDur(s.cfg.MaxBackoff, 5*time.Second), func() error {
			r, err := s.client.Do(req)
			if err != nil {
				return err
			}
			if r.StatusCode/100 != 2 {
				b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
				r.Body.Close()
				return fmt.Errorf("coindesk %d: %s", r.StatusCode, strings.TrimSpace(string(b)))
			}
			resp = r
			return nil
		})
		if err != nil {
			return nil, err
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		// Parse flexibly: { data: [...]} or { articles: [...] } or top-level array
		type article map[string]any
		flat := make([]article, 0, pageSize)
		var obj map[string]any
		if err := json.Unmarshal(body, &obj); err == nil && len(obj) > 0 {
			if v, ok := obj["data"]; ok {
				if arr, ok := v.([]any); ok {
					for _, it := range arr {
						if m, ok := it.(map[string]any); ok {
							flat = append(flat, m)
						}
					}
				}
			} else if v, ok := obj["articles"]; ok {
				if arr, ok := v.([]any); ok {
					for _, it := range arr {
						if m, ok := it.(map[string]any); ok {
							flat = append(flat, m)
						}
					}
				}
			}
		}
		if len(flat) == 0 {
			var arr []any
			if err := json.Unmarshal(body, &arr); err == nil {
				for _, it := range arr {
					if m, ok := it.(map[string]any); ok {
						flat = append(flat, m)
					}
				}
			}
		}
		if len(flat) == 0 {
			break
		}

		for _, m := range flat {
			id := pickStr(m, "id", "uuid", "_id")
			title := pickStr(m, "title", "headline")
			urlstr := pickStr(m, "url", "link", "permalink")
			summary := pickStr(m, "summary", "dek", "excerpt")

			var ts time.Time
			if s := pickStr(m, "published_at", "published_on", "date_published", "time_published"); s != "" {
				if t, err := parseTimeFlexible(s); err == nil {
					ts = t
				}
			}
			if ts.IsZero() {
				ts = time.Now().UTC()
			}

			labels := map[string]string{"news_source": "coindesk"}
			if cat := pickStr(m, "category"); cat != "" {
				labels["category"] = cat
			}
			if s.cfg.Language != "" {
				labels["language"] = s.cfg.Language
			}

			all = append(all, model.Event{
				ID:        id,
				Source:    s.Name(),
				Title:     title,
				Summary:   summary,
				URL:       urlstr,
				Published: ts,
				Lang:      s.cfg.Language,
				Country:   "",
				Raw:       m,
				Labels:    labels,
			})

			if d := ts.Format(time.RFC3339); d > latest {
				latest = d
			}
		}

		if s.cfg.StatePath != "" && latest != "" {
			_ = store.SaveNewsState(s.cfg.StatePath, store.NewsState{LastPublished: latest})
		}

		if len(flat) < pageSize {
			break
		}
	}

	return all, nil
}
