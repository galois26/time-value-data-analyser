package util

import (
	"context"
	"errors"
	"net"
	"net/http"
	"time"
)

func NewHTTPClient(timeout time.Duration) *http.Client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 60 * time.Second}).DialContext,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
	}
	return &http.Client{Timeout: timeout, Transport: tr}
}

// Simple exponential backoff with jitter-less growth.
func Retry(ctx context.Context, attempts int, initial, max time.Duration, fn func() error) error {
	if attempts <= 1 {
		return fn()
	}
	d := initial
	for i := 0; i < attempts; i++ {
		if i > 0 {
			select {
			case <-time.After(d):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := fn(); err != nil {
			if i == attempts-1 {
				return err
			}
			if d < max {
				d *= 2
				if d > max {
					d = max
				}
			}
			continue
		}
		return nil
	}
	return errors.New("retry: exhausted")
}
