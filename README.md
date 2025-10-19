# time-value-data-analyser
Data analyser used to store and present time series data

## File Structure

```
.git/                # Git version control directory
.github/             # GitHub related files
.gitignore           # Files to ignore in Git
README.md           # Project documentation
btc/                # Directory for Bitcoin data processing
monitoring/         # Directory for monitoring configurations
```

## How to Use

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/time-value-data-analyser.git
   cd time-value-data-analyser
   ```
2. Run the application:
   ```docker compose up
   ```
4. Access the grafana at `http://localhost:3000` .

5. Access prometheus metrics at `http://localhost:9090/metrics`
