package main

import (
    "fmt"
    "net/http"
    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
    sampleGauge = prometheus.NewGauge(prometheus.GaugeOpts{
        Name: "sample_wallet_balance",
        Help: "A sample gauge for wallet balance.",
    })
)

func main() {
    // Register Prometheus metrics
    prometheus.MustRegister(sampleGauge)

    // Set a sample value (simulate wallet balance)
    sampleGauge.Set(42.0)

    http.Handle("/metrics", promhttp.Handler())
    fmt.Println("Serving Prometheus metrics at :8080/metrics")
    if err := http.ListenAndServe(":8080", nil); err != nil {
        panic(err)
    }
}
