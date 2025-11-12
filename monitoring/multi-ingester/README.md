## Project Layout
```
multi-ingester/
├─ cmd/multi-ingester/main.go
├─ internal/
│  ├─ config/config.go
│  ├─ model/model.go
│  ├─ source/source.go           # Source interface + registry
│  ├─ source/gta.go              # GlobalTradeAlert implementation
│  ├─ postprocess/postprocess.go # Rule engine (keywords, regex, mapping)
│  ├─ sink/loki.go               # Push to Loki
│  ├─ sink/victoria.go           # Import Prom text to VictoriaMetrics
│  └─ util/httpclient.go
├─ go.mod
├─ go.sum
├─ Dockerfile
└─ config.example.yml
```