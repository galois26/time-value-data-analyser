package model

import "time"

// Event is the normalized representation for all sources.
type Event struct {
	ID        string // stable unique id for de-dup (from source)
	Source    string // e.g. "gta"
	Title     string
	Summary   string
	URL       string
	Published time.Time // when the event occurred / published
	Lang      string
	Country   string            // optional
	Raw       map[string]any    // original payload fields (for Loki JSON line)
	Labels    map[string]string // added/derived labels (post-process)
}
