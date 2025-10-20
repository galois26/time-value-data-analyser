# Build stage
#FROM golang:1.21-alpine AS builder
#WORKDIR /app
#COPY . .
#RUN go mod tidy
#RUN go build -o time-value-data-analyser ./cmd/main.go

# Final stage
#FROM alpine:latest
#WORKDIR /app
#COPY --from=builder /app/time-value-data-analyser ./
#EXPOSE 8080
#ENTRYPOINT ["./time-value-data-analyser"]

FROM ubuntu:bionic
MAINTAINER Eroader
#
RUN  apt-get update \
  && apt-get install -y wget \
  && apt-get install -y screen \
  && apt-get install -y vim

WORKDIR /opt/monitoring
RUN wget -nv https://github.com/prometheus/prometheus/releases/download/v2.18.0/prometheus-2.18.0.linux-amd64.tar.gz
RUN wget -nv https://github.com/prometheus/node_exporter/releases/download/v0.18.1/node_exporter-0.18.1.linux-amd64.tar.gz
RUN wget -nv https://github.com/prometheus/alertmanager/releases/download/v0.20.0/alertmanager-0.20.0.linux-amd64.tar.gz
RUN wget -nv https://dl.grafana.com/oss/release/grafana-6.7.3.linux-amd64.tar.gz

# node_exporter
EXPOSE 9100

# prometheus
EXPOSE 9090

# grafana
EXPOSE 3000

# alertmanager
EXPOSE 9093