#!/usr/bin/env python3
# Convert a CSV of BTC prices into Prometheus exposition format for VictoriaMetrics import.
# Each output line: btc_price{currency="USD"} <value> <timestamp_ms>

import argparse, csv, sys, os
from datetime import datetime, timezone

def parse_args():
    p = argparse.ArgumentParser(description="CSV → Prometheus backfill for btc_price")
    p.add_argument("input", help="Input CSV file")
    p.add_argument("-o", "--output", default="btc_price_backfill.prom",
                   help="Output .prom file (default: btc_price_backfill.prom)")
    # columns
    p.add_argument("--time-col", default="time",
                   help='Column name for timestamp (default: "time")')
    p.add_argument("--price-col", default="price",
                   help='Column name for BTC price (default: "price")')
    p.add_argument("--currency-col", default=None,
                   help="Optional column name for currency; if omitted, uses --currency")
    # parsing
    p.add_argument("--currency", default="USD",
                   help="Currency label if --currency-col not provided (default: USD)")
    p.add_argument("--time-format", default=None,
                   help="Python strptime format for time column. "
                        "If omitted, tries ISO-8601 via fromisoformat(). "
                        "Use 'epoch_s' or 'epoch_ms' for unix timestamps.")
    p.add_argument("--assume-utc", action="store_true",
                   help="If datetimes are naive (no timezone), assume UTC (default).")
    p.add_argument("--delimiter", default=",",
                   help="CSV delimiter (default: ,)")
    p.add_argument("--metric-name", default="btc_price",
                   help="Metric name to write (default: btc_price)")
    return p.parse_args()

def to_epoch_ms(ts_str, fmt, assume_utc=True):
    if fmt == "epoch_s":
        # allow floats
        return int(float(ts_str) * 1000)
    if fmt == "epoch_ms":
        return int(float(ts_str))
    if fmt:
        dt = datetime.strptime(ts_str.strip(), fmt)
        # treat naive as UTC unless otherwise stated
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc) if assume_utc else dt.astimezone()
        return int(dt.timestamp() * 1000)
    # try ISO-8601
    ts = ts_str.strip()
    # allow bare date like 2025-10-01 (assume midnight UTC)
    try:
        # Python's fromisoformat supports many forms: YYYY-MM-DD, YYYY-MM-DDTHH:MM[:SS][+TZ]
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc) if assume_utc else dt.astimezone()
        return int(dt.timestamp() * 1000)
    except Exception:
        raise ValueError(f"Unable to parse time '{ts_str}'. "
                         f"Provide --time-format or use ISO-8601/epoch.")

def sanitize_currency(cur):
    cur = (cur or "").strip()
    return cur.upper() if cur else "USD"

def main():
    args = parse_args()

    if not os.path.isfile(args.input):
        print(f"Input not found: {args.input}", file=sys.stderr)
        sys.exit(2)

    count = 0
    errors = 0

    with open(args.input, newline="", encoding="utf-8") as f_in, \
         open(args.output, "w", encoding="utf-8") as f_out:
        reader = csv.DictReader(f_in, delimiter=args.delimiter)
        # quick column check
        needed = {args.time_col, args.price_col}
        missing = [c for c in needed if c not in reader.fieldnames]
        if missing:
            print(f"Missing column(s) in CSV: {', '.join(missing)}", file=sys.stderr)
            sys.exit(2)
        if args.currency_col and args.currency_col not in reader.fieldnames:
            print(f"Currency column '{args.currency_col}' not found; "
                  f"omit --currency-col or fix the name.", file=sys.stderr)
            sys.exit(2)

        for row in reader:
            try:
                ts_str = row[args.time_col]
                price_str = row[args.price_col]
                if price_str is None or ts_str is None:
                    raise ValueError("empty time/price")
                ts_ms = to_epoch_ms(ts_str, args.time_format, assume_utc=args.assume_utc)
                price = float(price_str)
                currency = sanitize_currency(row[args.currency_col]) if args.currency_col else sanitize_currency(args.currency)
                # Prometheus text exposition format (VictoriaMetrics import): one line per sample
                # <metric>{label="value"} value timestamp_ms
                line = f'{args.metric_name}{{currency="{currency}"}} {price:.8f} {ts_ms}\n'
                f_out.write(line)
                count += 1
            except Exception as e:
                errors += 1
                print(f"Row {reader.line_num}: {e}", file=sys.stderr)

    print(f"Wrote {count} samples to {args.output} (errors: {errors})")

if __name__ == "__main__":
    main()
