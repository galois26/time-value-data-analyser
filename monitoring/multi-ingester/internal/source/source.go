package source

import (
	"context"
	"fmt"

	"time-value-analyser/multi-ingester/internal/config"
	"time-value-analyser/multi-ingester/internal/model"
)

type Source interface {
	Name() string
	Fetch(ctx context.Context) ([]model.Event, error)
}

func NewFromConfig(c config.SourceConfig) (Source, error) {
	switch c.Type {
	case "gta":
		return NewGTASource(c.GTA), nil
	case "coindesk":
		return NewCoinDeskSource(c.CoinDesk), nil
	default:
		return nil, fmt.Errorf("unknown source type: %s", c.Type)
	}
}
