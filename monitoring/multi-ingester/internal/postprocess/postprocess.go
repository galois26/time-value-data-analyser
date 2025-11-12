package postprocess

import (
	"regexp"
	"strings"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/model"
)

type Engine struct {
	kw   []config.KeywordRule
	regs []compiledRegex
	maps []config.MapRule
}

type compiledRegex struct {
	field  string
	re     *regexp.Regexp
	labels map[string]string
}

func New(cfg config.PostProcessConfig) (*Engine, error) {
	eng := &Engine{kw: cfg.Keywords, maps: cfg.Maps}
	for _, r := range cfg.Regex {
		re, err := regexp.Compile(r.Expr)
		if err != nil {
			return nil, err
		}
		eng.regs = append(eng.regs, compiledRegex{field: r.Field, re: re, labels: r.Labels})
	}
	return eng, nil
}

// Apply runs keyword, regex, and mapping rules over events and returns the mutated slice.
// If cfg has no rules, Apply returns the original events.
func Apply(events []model.Event, cfg config.PostProcessConfig) []model.Event {
	if len(events) == 0 {
		return events
	}

	// Pre-compile regex rules once
	type ritem struct {
		field  string
		re     *regexp.Regexp
		labels map[string]string
	}
	var rrules []ritem
	for _, rr := range cfg.Regex {
		if strings.TrimSpace(rr.Field) == "" || strings.TrimSpace(rr.Expr) == "" {
			continue
		}
		re, err := regexp.Compile(rr.Expr)
		if err != nil {
			continue
		}
		rrules = append(rrules, ritem{field: rr.Field, re: re, labels: rr.Labels})
	}

	// Normalize keyword rules
	type kitem struct {
		words  []string
		labels map[string]string
	}
	var krules []kitem
	for _, kr := range cfg.Keywords {
		if len(kr.When) == 0 {
			continue
		}
		words := make([]string, 0, len(kr.When))
		for _, w := range kr.When {
			if s := strings.TrimSpace(w); s != "" {
				words = append(words, strings.ToLower(s))
			}
		}
		if len(words) == 0 {
			continue
		}
		krules = append(krules, kitem{words: words, labels: kr.Labels})
	}

	// Map rules (simple value-to-label mapping for a given event field)
	type mitem struct {
		field   string
		outKey  string
		mapping map[string]string
	}
	var mrules []mitem
	for _, mr := range cfg.Maps {
		if strings.TrimSpace(mr.Field) == "" || len(mr.Mapping) == 0 {
			continue
		}
		out := mr.OutKey
		if out == "" {
			out = mr.Field
		}
		mrules = append(mrules, mitem{field: mr.Field, outKey: out, mapping: mr.Mapping})
	}

	// helper to get a string field from Event for rule evaluation
	getField := func(e *model.Event, name string) string {
		switch strings.ToLower(name) {
		case "title":
			return e.Title
		case "summary":
			return e.Summary
		case "url":
			return e.URL
		case "lang", "language":
			return e.Lang
		case "country":
			return e.Country
		default:
			// try label bag for arbitrary fields
			if v, ok := e.Labels[name]; ok {
				return v
			}
			return ""
		}
	}

	out := make([]model.Event, 0, len(events))

	for _, ev := range events {
		// Ensure Labels map is non-nil
		if ev.Labels == nil {
			ev.Labels = make(map[string]string, 4)
		}

		// 1) Keyword rules: if ALL words appear in title or summary (case-insensitive), apply labels
		titleLC := strings.ToLower(ev.Title)
		sumLC := strings.ToLower(ev.Summary)
		for _, kr := range krules {
			matched := true
			for _, w := range kr.words {
				if !strings.Contains(titleLC, w) && !strings.Contains(sumLC, w) {
					matched = false
					break
				}
			}
			if matched {
				for k, v := range kr.labels {
					ev.Labels[k] = v
				}
			}
		}

		// 2) Regex rules: run against specified field
		for _, rr := range rrules {
			val := getField(&ev, rr.field)
			if val == "" {
				continue
			}
			if rr.re.MatchString(val) {
				for k, v := range rr.labels {
					ev.Labels[k] = v
				}
			}
		}

		// 3) Map rules: if field value has a mapping, set the mapped label
		for _, mr := range mrules {
			val := getField(&ev, mr.field)
			if val == "" {
				continue
			}
			if mapped, ok := mr.mapping[val]; ok {
				ev.Labels[mr.outKey] = mapped
			}
		}

		out = append(out, ev)
	}

	return out
}
