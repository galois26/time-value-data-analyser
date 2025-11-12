package metrics

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

type labelKey string

type counterVec struct {
	mu   sync.Mutex
	data map[labelKey]float64
}

var (
	counters = struct {
		mu sync.Mutex
		m  map[string]*counterVec
	}{m: make(map[string]*counterVec)}
)

func keyFromLabels(labels map[string]string) labelKey {
	if len(labels) == 0 {
		return labelKey("")
	}
	ks := make([]string, 0, len(labels))
	for k := range labels {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	b := strings.Builder{}
	for i, k := range ks {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	return labelKey(b.String())
}

// IncCounter increments a named counter with a set of labels by v.
func IncCounter(name string, labels map[string]string, v float64) {
	counters.mu.Lock()
	cv, ok := counters.m[name]
	if !ok {
		cv = &counterVec{data: make(map[labelKey]float64)}
		counters.m[name] = cv
	}
	counters.mu.Unlock()

	lk := keyFromLabels(labels)
	cv.mu.Lock()
	defer cv.mu.Unlock()
	cv.data[lk] += v
}

// Dump returns a human-readable snapshot of counters (for logging).
func Dump() string {
	counters.mu.Lock()
	defer counters.mu.Unlock()
	var out []string
	for name, cv := range counters.m {
		cv.mu.Lock()
		for lk, v := range cv.data {
			out = append(out, fmt.Sprintf("%s{%s} %g", name, string(lk), v))
		}
		cv.mu.Unlock()
	}
	sort.Strings(out)
	return strings.Join(out, "")
}
