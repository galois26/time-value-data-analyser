package source

import (
	"fmt"
	"strings"
	"time"
)

// Small helper used by multiple sources to pick the first non-empty string key
func pickStr(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok {
				s2 := strings.TrimSpace(s)
				if s2 != "" {
					return s2
				}
			}
		}
	}
	return ""
}

// Parse timestamps in a few common formats (RFC3339, epoch seconds, common layouts)
func parseTimeFlexible(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t.UTC(), nil
	}
	// naive epoch seconds
	if len(s) >= 10 {
		allDigits := true
		for i := 0; i < len(s); i++ {
			if s[i] < '0' || s[i] > '9' {
				allDigits = false
				break
			}
		}
		if allDigits {
			var sec int64
			for i := 0; i < len(s); i++ {
				sec = sec*10 + int64(s[i]-'0')
			}
			return time.Unix(sec, 0).UTC(), nil
		}
	}
	for _, layout := range []string{"2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time: %s", s)
}

func defaultDur(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
