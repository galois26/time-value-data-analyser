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

	// Try a couple of common endpoints in order (first that returns rows wins)
	endpoints := []string{
		base + "/news/v1/article/list", // documented
		//base + "/content/v2/articles",  // some plans use this
	}

	// Time window: rolling > state > last 24h
	now := time.Now().UTC()
	var from, toStr time.Time
	if s.cfg.Window > 0 {
		from = now.Add(-s.cfg.Window)
		toStr = now
	} else if st, err := store.LoadNewsState(s.cfg.StatePath); err == nil && st.LastPublished != "" {
		if t, err := parseTimeFlexible(st.LastPublished); err == nil {
			from = t
		}
		toStr = now
	} else {
		from = now.Add(-24 * time.Hour)
		toStr = now
	}
	// Guard: if from after to (bad state file), reset to 24h window
	if from.After(toStr) {
		from = now.Add(-24 * time.Hour)
		toStr = now
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

	// try endpoints in order
	for _, endpoint := range endpoints {
		gotAny := false

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

			// Try multiple time param styles
			// Most APIs are lenient and ignore unknown params.
			q.Set("from", from.Format(time.RFC3339))
			q.Set("to", toStr.Format(time.RFC3339))
			q.Set("published_after", from.Format(time.RFC3339))
			q.Set("published_before", toStr.Format(time.RFC3339))

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

			// DEBUG: show request URL once per page
			fmt.Printf("coindesk: GET %s\n", u.String())

			var resp *http.Response
			err = util.Retry(ctx, max(1, s.cfg.MaxRetries), defaultDur(s.cfg.Backoff, 500*time.Millisecond), defaultDur(s.cfg.MaxBackoff, 5*time.Second), func() error {
				r, err := s.client.Do(req)
				if err != nil {
					return err
				}
				if r.StatusCode/100 != 2 {
					b, _ := io.ReadAll(io.LimitReader(r.Body, 1024))
					r.Body.Close()
					return fmt.Errorf("coindesk %d: %s", r.StatusCode, strings.TrimSpace(string(b)))
				}
				resp = r
				return nil
			})
			if err != nil {
				// If this endpoint shape fails, try next endpoint
				fmt.Printf("coindesk: request error: %v\n", err)
				break
			}

			raw, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}

			// DEBUG: body head
			if len(raw) > 0 {
				head := raw
				if len(head) > 400 {
					head = head[:400]
				}
				fmt.Printf("coindesk: resp head: %s\n", string(head))
			}

			// Parse many common shapes
			// Shapes we support:
			// 1) {"data":[{...}, ...]}

			// 2) {"articles":[{...}, ...]}
			// 3) {"items":[{...}, ...]}
			// 4) {"results":{"docs":[{...}, ...]}}
			// 5) top-level array [{...}, ...]
			flat := make([]map[string]any, 0, pageSize)
			var o1 map[string]any
			if json.Unmarshal(raw, &o1) == nil && len(o1) > 0 {
				// CoinDesk Data API: { "Data": [ ... ] }
				if v, ok := o1["Data"]; ok {
					if arr, ok := v.([]any); ok {
						for _, it := range arr {
							if m, ok := it.(map[string]any); ok {
								flat = append(flat, m)
							}
						}
					}
				}

				// Generic shapes we already supported
				for _, key := range []string{"data", "articles", "items"} {
					if arrAny, ok := o1[key].([]any); ok {
						for _, it := range arrAny {
							if m, ok := it.(map[string]any); ok {
								flat = append(flat, m)
							}
						}
					}
				}
				if res, ok := o1["results"].(map[string]any); ok {
					if docs, ok := res["docs"].([]any); ok {
						for _, it := range docs {
							if mm, ok := it.(map[string]any); ok {
								flat = append(flat, mm)
							}
						}
					}
				}
			}
			if len(flat) == 0 {
				// top-level array fallback
				var a []any
				if json.Unmarshal(raw, &a) == nil {
					for _, it := range a {
						if m, ok := it.(map[string]any); ok {
							flat = append(flat, m)
						}
					}
				}
			}
			fmt.Printf("coindesk: parsed rows page=%d endpoint=%s -> %d\n", page, endpoint, len(flat))
			if len(flat) == 0 {
				if page == 1 { /* try next endpoint */
				}
				break
			}

			// Map to events — handle UPPERCASE (Data API) and lowercase (generic)
			for i, m := range flat {
				// ID: may be numeric
				id := pickStr(m, "id", "uuid", "_id", "ID")
				if id == "" {
					if v, ok := m["ID"]; ok {
						id = fmt.Sprint(v)
					}
				}

				title := pickStr(m, "title", "headline", "name", "TITLE")
				urlstr := pickStr(m, "url", "link", "permalink", "URL", "GUID")
				summary := pickStr(m, "summary", "dek", "excerpt", "description", "SUBTITLE")

				// published time
				var ts time.Time
				if s := pickStr(m, "published_at", "publish_date", "published_on", "date_published", "time_published"); s != "" {
					if t, err := parseTimeFlexible(s); err == nil {
						ts = t
					}
				}
				if ts.IsZero() {
					// CoinDesk Data API: epoch seconds in PUBLISHED_ON
					if v, ok := m["PUBLISHED_ON"]; ok {
						switch vv := v.(type) {
						case float64:
							ts = time.Unix(int64(vv), 0).UTC()
						case int64:
							ts = time.Unix(vv, 0).UTC()
						case json.Number:
							if sec, err := vv.Int64(); err == nil {
								ts = time.Unix(sec, 0).UTC()
							}
						}
					}
				}
				if ts.IsZero() {
					ts = now
				}
				if ts.After(latest) {
					latest = ts
				}

				labels := map[string]string{"news_source": "coindesk"}
				if cat := pickStr(m, "category", "section", "TYPE"); cat != "" {
					labels["category"] = cat
				}
				if s.cfg.Language != "" {
					labels["language"] = s.cfg.Language
				}

				// tags (optional)
				if tv, ok := m["tags"]; ok {
					switch t := tv.(type) {
					case []any:
						parts := make([]string, 0, len(t))
						for _, it := range t {
							parts = append(parts, fmt.Sprint(it))
						}
						if len(parts) > 0 {
							labels["tags"] = strings.Join(parts, ",")
						}
					case string:
						if t != "" {
							labels["tags"] = t
						}
					}
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

				if i == 0 {
					fmt.Printf("coindesk: first mapped: id=%s ts=%s\n", id, ts.Format(time.RFC3339))
				}
			}
			// advance page; if fewer than pageSize, stop
			if len(flat) < pageSize {
				break
			}
		}

		if gotAny {
			// Save cursor once per endpoint
			if s.cfg.StatePath != "" && !latest.IsZero() {
				_ = store.SaveNewsState(s.cfg.StatePath, store.NewsState{LastPublished: latest.Format(time.RFC3339)})
			}
			break // we got data from this endpoint; don't try the next
		}
	}

	return all, nil
}
