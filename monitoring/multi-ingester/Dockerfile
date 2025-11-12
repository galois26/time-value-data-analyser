# ---- Build --------------------------------------------------------------
FROM golang:1.23-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates build-base
COPY go.mod go.sum ./
ENV GOTOOLCHAIN=auto
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
go build -v -trimpath -tags netgo,osusergo -ldflags "-s -w" \
-o /out/multi-ingester ./cmd


# ---- Runtime ------------------------------------------------------------
FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/multi-ingester /usr/local/bin/multi-ingester
COPY config.yml /config.yml
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/multi-ingester", "-config", "/config.yml", "-interval", "15m"]
