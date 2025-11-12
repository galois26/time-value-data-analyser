package source

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/metrics"
	"time-value-analyser/multi-ingester/internal/model"
	"time-value-analyser/multi-ingester/internal/store"
	"time-value-analyser/multi-ingester/internal/util"
)

type gtaSource struct {
	cfg    config.GTAConfig
	client *http.Client
}

func NewGTASource(cfg config.GTAConfig) *gtaSource {
	to := cfg.HTTP.Timeout
	if to == 0 {
		to = 15 * time.Second
	}
	return &gtaSource{cfg: cfg, client: util.NewHTTPClient(to)}
}

func (g *gtaSource) Name() string { return "gta" }

// Fetch retrieves events from the GTA API within the configured time window.
func (g *gtaSource) Fetch(ctx context.Context) ([]model.Event, error) {
	base := strings.TrimRight(g.cfg.BaseURL, "/")
	if base == "" {
		base = "https://api.globaltradealert.org"
	}
	// NOTE: GTA spec path block defines v1; many deployments accept v1 with V2 body.
	endpoint := base + "/api/v2/gta/data/"

	// Build time window
	var from, toStr string
	if g.cfg.Window > 0 {
		from = time.Now().UTC().Add(-g.cfg.Window).Format("2006-01-02")
		toStr = time.Now().UTC().Format("2006-01-02")
	} else if st, err := store.LoadGTAState(g.cfg.StatePath); err == nil && st.LastAnnounced != "" {
		from = st.LastAnnounced
		toStr = time.Now().UTC().Format("2006-01-02")
	} else if g.cfg.Since != "" {
		from = g.cfg.Since
		toStr = time.Now().UTC().Format("2006-01-02")
	} else {
		from = time.Now().UTC().Add(-24 * time.Hour).Format("2006-01-02")
		toStr = time.Now().UTC().Format("2006-01-02")
	}

	// GTA uses different date filters; try primary first, then fallbacks
	primary := strings.TrimSpace(g.cfg.DateFilter)
	if primary == "" {
		primary = "announcement_period"
	}

	// build the rolling/state window (you already compute from/to as dates YYYY-MM-DD)
	filters := []string{primary}
	fallbackOrder := []string{"announcement_period", "update_period", "submission_period"}
	for _, f := range fallbackOrder {
		if f != primary {
			filters = append(filters, f)
		}
	}

	limit := 200
	offset := 0
	var all []model.Event
	latest := from

	for {
		var flat []map[string]any
		usedFilter := ""
		for _, key := range filters {
			var requestData map[string]any
			// first attempt uses configured key; if that yields empty, we’ll retry with next key
			if key == "in_force_on_date" {
				requestData = map[string]any{"in_force_on_date": toStr} // snapshot on the end date
			} else {
				requestData = map[string]any{key: []string{from, toStr}}
			}

			body := map[string]any{
				"limit":        limit,
				"offset":       offset,
				"sorting":      "-intervention_id",
				"request_data": requestData,
			}

			raw, err := json.Marshal(body)
			log.Printf("gta body: %s", raw)
			if err != nil {
				return nil, err
			}
			// Fresh request for every retry attempt to avoid drained Body issues.
			mkReq := func() (*http.Request, error) {
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
				if err != nil {
					return nil, err
				}
				req.Header.Set("Content-Type", "application/json")
				if s := strings.TrimSpace(g.cfg.APIKey); s != "" {
					req.Header.Set("Authorization", "APIKey "+s)
				}
				if ua := g.cfg.HTTP.UserAgent; ua != "" {
					req.Header.Set("User-Agent", ua)
				}
				return req, nil
			}

			// Do request with retries
			var resp *http.Response
			err = util.Retry(ctx, max(1, g.cfg.MaxRetries), defaultDur(g.cfg.Backoff, 500*time.Millisecond), defaultDur(g.cfg.MaxBackoff, 5*time.Second), func() error {
				req, err := mkReq()
				if err != nil {
					return err
				}
				r, err := g.client.Do(req)
				if err != nil {
					return err
				}
				if r.StatusCode/100 == 4 {
					b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
					r.Body.Close()
					return fmt.Errorf("gta quota: %s", strings.TrimSpace(string(b)))
				}
				if r.StatusCode/100 != 2 {
					b, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
					r.Body.Close()
					return fmt.Errorf("gta %d: %s", r.StatusCode, strings.TrimSpace(string(b)))
				}
				resp = r
				log.Printf("gta: Status Code:  %v", resp.StatusCode)
				return nil
			})
			if err != nil {
				if strings.HasPrefix(err.Error(), "gta quota:") && len(all) > 0 {
					break
				}
				return nil, err
			}

			// Read once; log and parse from the same bytes
			rawBody, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				return nil, err
			}

			// Optional: verbose logging of first bytes, without consuming the stream twice
			if len(rawBody) > 0 {
				max := 512
				if len(rawBody) < max {
					max = len(rawBody)
				}
				fmt.Printf("gta: Status Code: %d\n", resp.StatusCode)
				fmt.Printf("gta: Response Body (first %dB): %s\n", max, string(rawBody[:max]))
			}

			// Parse response

			flat = make([]map[string]any, 0, 1024)
			// Try parsing with results/count wrapper
			var obj struct {
				Count   int `json:"count"`
				Results any `json:"results"`
			}

			if err := json.Unmarshal(rawBody, &obj); err == nil && (obj.Results != nil || obj.Count > 0) {
				if arr, ok := obj.Results.([]any); ok {
					for _, it := range arr {
						if m, ok := it.(map[string]any); ok {
							flat = append(flat, m)
						} else if subArr, ok := it.([]any); ok {
							for _, sub := range subArr {
								if m2, ok := sub.(map[string]any); ok {
									flat = append(flat, m2)
								}
							}
						}
					}
				}
			} else {
				var arr []any
				if err := json.Unmarshal(rawBody, &arr); err == nil {
					for _, it := range arr {
						if m, ok := it.(map[string]any); ok {
							flat = append(flat, m)
						}
					}
					// single page if top-level array
					offset = 0
				} else {
					return nil, fmt.Errorf("gta: unrecognized response shape (len=%d)", len(rawBody))
				}
			}

			fmt.Printf("gta: parsed rows this page: %d\n", len(flat))
			if len(flat) == 0 {
				break
			}
			// DEBUG: show parsed row count

			// Metrics: count rows by filter used
			metrics.IncCounter("gta_events_total", map[string]string{"filter": usedFilter}, float64(len(flat)))

			// ---- map rows -> events ----
			mappedThisPage := 0
			for i, m := range flat {
				id := fmt.Sprint(m["intervention_id"])
				title := fmt.Sprint(m["state_act_title"])
				url := fmt.Sprint(m["intervention_url"])

				var ts time.Time
				if s, _ := m["date_published"].(string); s != "" {
					if t, err := time.Parse("2006-01-02", s); err == nil {
						ts = t.UTC()
					}
				}
				if ts.IsZero() {
					if s, _ := m["date_announced"].(string); s != "" {
						if t, err := time.Parse("2006-01-02", s); err == nil {
							ts = t.UTC()
						}
					}
				}
				if ts.IsZero() {
					ts = time.Now().UTC()
				}
				if d := ts.Format("2006-01-02"); d > latest {
					latest = d
				}

				iso, implName := pickImplementer(m)
				labels := map[string]string{}
				if v := fmt.Sprint(m["gta_evaluation"]); v != "" {
					labels["gta_evaluation"] = v
				}
				if v := fmt.Sprint(m["implementation_level"]); v != "" {
					labels["implementation_level"] = v
				}
				if v := fmt.Sprint(m["intervention_type"]); v != "" {
					labels["intervention_type"] = v
				}
				if iso != "" {
					labels["implementer_iso"] = iso
				}
				if implName != "" {
					labels["implementer_name"] = implName
				}

				// 'all' must be declared once above the pagination loop: var all []model.Event
				all = append(all, model.Event{
					ID:        id,
					Source:    g.Name(),
					Title:     title,
					Summary:   "",
					URL:       url,
					Published: ts,
					Lang:      "",
					Country:   iso,
					Raw:       m,
					Labels:    labels,
				})

				mappedThisPage++
				if i == 0 {
					fmt.Printf("gta: first mapped event id=%s ts=%s\n", id, ts.Format(time.RFC3339))
				}
			}
			fmt.Printf("gta: mapped %d events this page, total so far %d\n", mappedThisPage, len(all))

			offset += len(flat)

			usedFilter = key
			if len(flat) > 0 {
				break
			}
		}

		if len(flat) == 0 {
			break
		}
	}
	// Update state file with latest announced date
	if g.cfg.StatePath != "" && latest != "" {
		st := store.GTAState{LastAnnounced: latest}
		if err := store.SaveGTAState(g.cfg.StatePath, st); err != nil {
			log.Printf("gta: save state: %v", err)
		}
	}

	return all, nil
}

// pickImplementer tries multiple GTA shapes to find a primary implementer.
// It returns (iso, name). iso is ISO3 if available, else ISO2, uppercased.
func pickImplementer(m map[string]any) (string, string) {
	if v, ok := m["implementing_jurisdictions"]; ok {
		if arr, ok := v.([]any); ok && len(arr) > 0 {
			if obj, ok := arr[0].(map[string]any); ok {
				iso := ""
				if s, _ := obj["iso3"].(string); s != "" {
					iso = s
				}
				if iso == "" {
					if s, _ := obj["iso2"].(string); s != "" {
						iso = s
					}
				}
				name := ""
				if s, _ := obj["name"].(string); s != "" {
					name = s
				}
				return strings.ToUpper(iso), name
			}
		}
	}
	if v, ok := m["implementers"]; ok {
		switch tt := v.(type) {
		case []any:
			if len(tt) > 0 {
				switch first := tt[0].(type) {
				case string:
					return strings.ToUpper(first), ""
				case map[string]any:
					iso := ""
					if s, _ := first["iso3"].(string); s != "" {
						iso = s
					}
					if iso == "" {
						if s, _ := first["iso2"].(string); s != "" {
							iso = s
						}
					}
					if iso == "" {
						if s, _ := first["code"].(string); s != "" {
							iso = s
						}
					}
					name := ""
					if s, _ := first["name"].(string); s != "" {
						name = s
					}
					return strings.ToUpper(iso), name
				}
			}
		}
	}
	if s, _ := m["implementer_iso3"].(string); s != "" {
		return strings.ToUpper(s), ""
	}
	if s, _ := m["implementer_iso2"].(string); s != "" {
		return strings.ToUpper(s), ""
	}
	return "", ""
}
