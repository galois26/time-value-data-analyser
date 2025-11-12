package sink

import (
	"context"
	"time-value-analyser/multi-ingester/internal/model"
)

// Sink is the minimal interface all sinks must implement.
type Sink interface {
	Name() string
	Push(ctx context.Context, events []model.Event) error
}
