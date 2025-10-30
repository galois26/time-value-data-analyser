package price

import (
	"context"
)

type Price struct {
	Currency string  // e.g., USD
	Value    float64 // price of 1 BTC in currency
}

type PriceProvider interface {
	GetBTCPrice(ctx context.Context, currency string) (Price, error)
	Name() string
}
