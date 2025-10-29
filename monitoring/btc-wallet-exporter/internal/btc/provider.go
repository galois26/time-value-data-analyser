package btc

import "context"

// Balance represents a wallet's balance in satoshis.
// You could add fields for received/spent if needed later.

type Balance struct {
	Address string
	Sats    uint64
}

// BalanceProvider fetches balances for BTC addresses.
// Different implementations can use block explorers or a full node.

type BalanceProvider interface {
	GetBalance(ctx context.Context, address string, includeMempool bool) (Balance, error)
	Name() string
}
