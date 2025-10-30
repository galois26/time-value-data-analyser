.PHONY: run build docker


build:
	go build -o bin/btc-wallet-exporter ./cmd/btc-wallet-exporter


run:
	go run ./cmd/btc-wallet-exporter -config config.example.yml -interval 30s


docker:
	docker build -t btc-wallet-exporter:latest .