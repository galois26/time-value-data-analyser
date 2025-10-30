package price

import (
	"fmt"

	"btc-wallet-exporter/internal/config"
)

func NewProviderFromConfig(pc config.Price) (PriceProvider, error) {
	switch pc.Provider.Type {
	case "coingecko":
		return NewCoinGecko(
			pc.Provider.BaseURL,
			pc.Provider.APIKey, // <- API key flows in
			pc.Provider.UserAgent,
			pc.Provider.Timeout,
		), nil
	case "":
		return nil, fmt.Errorf("price.provider.type is required")
	default:
		return nil, fmt.Errorf("unknown price provider: %s", pc.Provider.Type)
	}
}
