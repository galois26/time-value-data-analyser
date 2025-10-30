# ---- Build stage -----------------------------------------------------------
FROM golang:1.23-alpine AS build
WORKDIR /src

# Tools & CA certs (we'll copy certs into scratch at runtime)
RUN apk add --no-cache ca-certificates build-base

# Copy mod files first for caching
COPY go.mod go.sum ./
# Let Go auto-fetch exact toolchain if you have a `toolchain go1.23.x` line
ENV GOTOOLCHAIN=auto
RUN go mod download

# Copy the rest
COPY . .

# Sanity: show module graph & environment (helps diagnose)
RUN go env && go list -m all | sort | sed -n '1,200p'

# Build a static binary (pure Go) — use -v for verbose errors
# NOTE: We do NOT pass -extldflags=-static when CGO_ENABLED=0; it's unnecessary and can mask errors.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -v -trimpath \
    -tags netgo,osusergo \
    -ldflags "-s -w" \
    -o /out/btc-wallet-exporter ./cmd/btc-wallet-exporter

# ---- Runtime stage (Option A: scratch, tiny) -------------------------------
FROM scratch AS runtime
# HTTPS roots
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# Binary
COPY --from=build /out/btc-wallet-exporter /usr/local/bin/btc-wallet-exporter
# Default config path (you’ll bind-mount your config.yml at runtime)
COPY config.example.yml /config.yml
EXPOSE 9108
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/btc-wallet-exporter", "-config", "/config.yml"]
