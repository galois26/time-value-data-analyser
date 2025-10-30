#!/usr/bin/env python3
"""
newsdata_to_loki.py
Fetch latest news from newsdata.io and push them to a Grafana Loki endpoint.

Usage examples:
  python3 newsdata_to_loki.py --api-key $NEWSDATA_API_KEY --q bitcoin --loki-url http://localhost:3100
  python3 newsdata_to_loki.py --api-key $KEY --q "crypto OR bitcoin" --pages 3 --tenant main --labels env=dev team=data
  python3 newsdata_to_loki.py --api-key $KEY --category business --country us --language en --dry-run

Requirements:
  - Python 3.8+
  - requests (install with: pip install requests)

Notes:
  - This script creates one Loki log line per article, encoded as compact JSON.
  - Labels kept low-cardinality by default: job=newsdata, source, language, q (query).
  - You can add custom static labels via --labels key=value key=value ...
"""

import argparse
import json
import os
import sys
import time
from typing import Dict, Any, Iterable, List, Tuple
from urllib.parse import urlencode

# Optional dependency: requests. We fall back to urllib if not available.
try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

# try to import VADER
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer  # type: ignore
except Exception:
    SentimentIntensityAnalyzer = None  # type: ignore

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


DEFAULT_BASE_URL = "https://newsdata.io/api/1/latest"
DEFAULT_LOKI_PATH = "/loki/api/v1/push"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fetch latest news from newsdata.io and push to Loki")
    p.add_argument("--api-key", required=True, help="newsdata.io API key (or set NEWSDATA_API_KEY)")
    p.add_argument("--base-url", default=DEFAULT_BASE_URL, help=f"newsdata latest endpoint (default: {DEFAULT_BASE_URL})")
    # common filters from newsdata.io (subset)
    p.add_argument("--q", help="Query string, e.g. 'bitcoin OR crypto'")
    p.add_argument("--country", help="Country code(s), comma-separated, e.g. 'us,gb'")
    p.add_argument("--category", help="Category(ies), e.g. 'business,technology'")
    p.add_argument("--language", help="Language code(s), e.g. 'en,de'")
    p.add_argument("--domain", help="Restrict to domains, comma-separated")
    p.add_argument("--page", type=int, default=None, help="Start page")
    p.add_argument("--pages", type=int, default=1, help="How many pages to fetch (default: 1)")
    p.add_argument("--page-size", type=int, default=None, help="Page size if supported by your plan")
    # Loki
    p.add_argument("--loki-url", required=True, help="Loki base URL, e.g. http://localhost:3100")
    p.add_argument("--tenant", help="X-Scope-OrgID header value for multi-tenant Loki")
    p.add_argument("--job", default="newsdata", help="Loki label: job (default: newsdata)")
    p.add_argument("--labels", nargs="*", default=[], help="Extra static labels key=value (space-separated)")
    # behavior
    # in parse_args()
    p.add_argument("--timeout", type=float, default=15.0, help="HTTP timeout seconds (default: 15)")
    p.add_argument("--dry-run", action="store_true", help="Do not push to Loki; print payload preview")
    p.add_argument("--verbose", action="store_true", help="Verbose logging to stderr")
    # Sentiment analysis
    p.add_argument("--use-sentiment", action="store_true", help="Compute VADER sentiment; adds sentiment fields to each log line")
    p.add_argument("--vm-url", help="VictoriaMetrics base URL, e.g. http://victoria-metrics:8428")
    p.add_argument("--emit-metrics", action="store_true",help="Compute and send sentiment averages to VictoriaMetrics")
    return p.parse_args()


def log(msg: str, verbose: bool = True):
    if verbose:
        sys.stderr.write(msg.rstrip() + "\n")


def http_get_json(url: str, params: Dict[str, Any], timeout: float) -> Dict[str, Any]:
    if requests is None:
        from urllib.request import urlopen, Request
        full = url + "?" + urlencode(params)
        req = Request(full, headers={"User-Agent": "newsdata-to-loki/1.0"})
        with urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            return json.loads(data.decode("utf-8"))
    else:
        r = requests.get(url, params=params, timeout=timeout, headers={"User-Agent": "newsdata-to-loki/1.0"})
        if r.status_code == 422:
            # Show server error body to help debug invalid params
            raise requests.HTTPError(f"422 from newsdata.io: {r.text}", response=r)
        r.raise_for_status()
        return r.json()


def http_post_json(url: str, payload: Dict[str, Any], timeout: float, tenant: str = None) -> Tuple[int, str]:
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    if requests is None:
        from urllib.request import urlopen, Request
        headers = {"Content-Type": "application/json"}
        if tenant:
            headers["X-Scope-OrgID"] = tenant
        req = Request(url, data=body, headers=headers, method="POST")
        with urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            text = resp.read().decode("utf-8", errors="replace")
            return status, text
    else:
        headers = {"Content-Type": "application/json"}
        if tenant:
            headers["X-Scope-OrgID"] = tenant
        r = requests.post(url, data=body, headers=headers, timeout=timeout)
        return r.status_code, r.text


def to_ns(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000_000)

def sentiment_score(text: str) -> float:
    if not text:
        return 0.0
    if SentimentIntensityAnalyzer is None:
        return 0.0
    analyzer = SentimentIntensityAnalyzer()
    return float(analyzer.polarity_scores(text)["compound"])

def label_from_score(s: float) -> str:
    if s >= 0.2:
        return "pos"
    if s <= -0.2:
        return "neg"
    return "neu"

def now_ms() -> int:
    from datetime import datetime, timezone
    return int(datetime.now(timezone.utc).timestamp() * 1000)

def to_prom_line(metric: str, labels: dict, value: float, ts_ms: int) -> str:
    # Escape label values minimally (quotes and backslashes)
    lab = ",".join(f'{k}="{str(v).replace("\\", "\\\\").replace("\"","\\\"")}"' for k,v in sorted(labels.items()))
    return f'{metric}{{{lab}}} {value} {ts_ms}\n'

def parse_pubdate(s: str) -> datetime:
    """
    Try robust parsing for various formats used by news feeds:
    - ISO 8601 with/without Z
    - RFC 2822 (via email.utils)
    - 'YYYY-MM-DD HH:MM:SS' (assume UTC)
    """
    if not s:
        return datetime.now(timezone.utc)
    s2 = s.strip()
    # Normalize Z
    if s2.endswith("Z"):
        try:
            return datetime.fromisoformat(s2.replace("Z", "+00:00"))
        except Exception:
            pass
    # Plain ISO or with offset
    try:
        dt = datetime.fromisoformat(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        pass
    # RFC 2822
    try:
        dt = parsedate_to_datetime(s2)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        pass
    # Fallback: try 'YYYY-MM-DD HH:MM:SS'
    try:
        dt = datetime.strptime(s2, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def article_to_line(obj: Dict[str, Any], add_sentiment: bool = False) -> str:
    fields = {}
    for k in ("title", "link", "source_id", "pubDate", "creator", "description", "content", "category", "country", "language"):
        if k in obj and obj[k] is not None:
            fields[k] = obj[k]
    for k in ("description", "content"):
        if k in fields and isinstance(fields[k], str) and len(fields[k]) > 2000:
            fields[k] = fields[k][:2000] + "..."
    if add_sentiment:
        title = fields.get("title") or ""
        desc = fields.get("description") or ""
        s = sentiment_score((title + " " + desc).strip())
        fields["sentiment"] = round(s, 4)
        fields["sentiment_label"] = label_from_score(s)
    return json.dumps(fields, ensure_ascii=False, separators=(",", ":"))

def group_by_stream(articles: Iterable[Dict[str, Any]], base_labels: Dict[str, str], ts_ingest: bool = False, add_sentiment: bool = False) -> Dict[Tuple[Tuple[str, str], ...], List[Tuple[int, str]]]:
    from collections import defaultdict
    streams = defaultdict(list)
    for a in articles:
        source = a.get("source_id") or "unknown"
        lang = a.get("language") or "unknown"
        pub = a.get("pubDate") or a.get("pub_date") or a.get("date") or ""
        ts = to_ns(datetime.now(timezone.utc)) if ts_ingest else to_ns(parse_pubdate(pub))
        line = article_to_line(a, add_sentiment=add_sentiment)
        stream_labels = dict(base_labels)
        stream_labels.update({"source": str(source), "language": str(lang)})
        key = tuple(sorted(stream_labels.items()))
        streams[key].append((ts, line))
    return streams

def build_loki_payload(streams: Dict[Tuple[Tuple[str, str], ...], List[Tuple[int, str]]]) -> Dict[str, Any]:
    out = {"streams": []}
    for key, values in streams.items():
        labels = dict(key)
        # Loki expects timestamps as strings
        vals = [[str(ts), line] for ts, line in sorted(values, key=lambda t: t[0])]
        out["streams"].append({"stream": labels, "values": vals})
    return out


def fetch_news(args: argparse.Namespace) -> List[Dict[str, Any]]:
    params = {
        "apikey": args.api_key or os.environ.get("NEWSDATA_API_KEY", ""),
    }
    # Optional filters
    for k in ("q", "country", "category", "language", "domain"):
        v = getattr(args, k, None)
        if v:
            params[k] = v
    if args.page_size:
        params["page_size"] = args.page_size

    # Newsdata pagination model:
    # - First request: DO NOT send 'page'
    # - Response may include 'nextPage' (string). For the next request, send page=<that exact token>.
    # - Repeat until 'nextPage' is absent.
    pages_to_fetch = max(1, args.pages)
    next_page_token = args.page  # allow starting from a token (string) if user passes --page
    articles: List[Dict[str, Any]] = []
    seen_tokens = set()

    for i in range(pages_to_fetch):
        req_params = dict(params)
        if next_page_token:  # send only when token is present
            req_params["page"] = str(next_page_token)

        try:
            data = http_get_json(args.base_url, req_params, timeout=args.timeout)
        except requests.HTTPError as e:
            # Graceful stop if token invalid or unsupported
            if e.response is not None and e.response.status_code == 422:
                # Most often "The provided value for the next page is invalid"
                break
            raise

        batch = data.get("results") or data.get("articles") or []
        if isinstance(batch, list):
            articles.extend(batch)

        token = data.get("nextPage") or data.get("next_page") or None
        if not token:
            break
        # Prevent loops if API ever repeats a token
        if token in seen_tokens:
            break
        seen_tokens.add(token)
        next_page_token = token

    return articles

def parse_static_labels(items: List[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in items:
        if "=" in item:
            k, v = item.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def main():
    args = parse_args()
    if not args.api_key:
        print("Missing --api-key", file=sys.stderr)
        sys.exit(2)

    base_labels = {"job": args.job}
    extra = parse_static_labels(args.labels)
    base_labels.update(extra)
    if args.q:
        base_labels["q"] = args.q

    # --- fetch with defensive init ---
    articles = []
    try:
        log(f"Fetching news from {args.base_url} ...", args.verbose)
        articles = fetch_news(args)
        log(f"Fetched {len(articles)} articles", args.verbose)
    except Exception as e:
        # Show a concise reason and stop; avoids using an undefined variable
        log(f"ERROR: fetch failed: {e}", True)
        sys.exit(1)

    # ---- Build sentiment metrics and send to VictoriaMetrics (optional) ----
    if args.emit_metrics and args.vm_url:
        if not getattr(args, "use_sentiment", False):
            log("emit-metrics requested but --use-sentiment is off; skipping metrics", True)
        else:
            from collections import defaultdict
            try:
                from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
                analyzer = SentimentIntensityAnalyzer()
            except Exception:
                analyzer = None

            def compute_sentiment(a):
                if analyzer is None:
                    return None
                title = (a.get("title") or "").strip()
                desc  = (a.get("description") or "").strip()
                text = (title + " " + (" " + desc if desc else "")).strip()
                if not text:
                    return None
                return float(analyzer.polarity_scores(text)["compound"])

            # 1) group sums/counts
            groups = defaultdict(lambda: {"sum": 0.0, "n": 0})
            for a in articles:
                s = compute_sentiment(a)
                if s is None:
                    continue
                source = a.get("source_id") or "unknown"
                lang   = a.get("language") or "unknown"
                groups[(source, lang)]["sum"] += s
                groups[(source, lang)]["n"]   += 1

            # 2) base labels
            base_labels = {"job": args.job}
            base_labels.update(parse_static_labels(args.labels))
            if args.q:
                base_labels["q"] = args.q

            # 3) emit prom text
            ts_ms = now_ms()
            prom_lines = []
            for (source, lang), agg in groups.items():
                if agg["n"] == 0:
                    continue
                labels = dict(base_labels)
                labels["source"]   = source
                labels["language"] = lang
                avg = agg["sum"] / agg["n"]
                prom_lines.append(to_prom_line("news_sentiment_avg",   labels, round(avg, 6), ts_ms))
                prom_lines.append(to_prom_line("news_sentiment_count", labels, agg["n"],       ts_ms))

            if prom_lines:
                vm_import = args.vm_url.rstrip("/") + "/api/v1/import/prometheus"
                body = "".join(prom_lines).encode("utf-8")
                try:
                    if requests is None:
                        from urllib.request import Request, urlopen
                        req = Request(vm_import, data=body, headers={"Content-Type": "text/plain"}, method="POST")
                        with urlopen(req, timeout=args.timeout) as resp:
                            log(f"VM import: status {resp.getcode()} (samples: {len(prom_lines)})", True)
                    else:
                        r = requests.post(vm_import, data=body, headers={"Content-Type": "text/plain"}, timeout=args.timeout)
                        if r.status_code // 100 != 2:
                            log(f"VM import error {r.status_code}: {r.text}", True)
                        else:
                            log(f"VM import: OK ({len(prom_lines)} samples)", True)
                except Exception as e:
                    log(f"VM import failed: {e}", True)
            else:
                log("No sentiment rows to emit (no articles with title/description or VADER unavailable)", True)

    # Timestamp mode: default pubDate; allow ingest-time for debugging
    ts_ingest = getattr(args, "ts_ingest", False)

    streams = group_by_stream(
        articles,
        base_labels,
        ts_ingest=ts_ingest,
        add_sentiment=getattr(args, "use_sentiment", False),
    )
    payload = build_loki_payload(streams)

    if args.dry_run:
        preview = json.dumps(payload, ensure_ascii=False)[:1000]
        print(preview + ("..." if len(preview) == 1000 else ""))
        return

    loki_push = args.loki_url.rstrip("/") + DEFAULT_LOKI_PATH
    log(f"Pushing to Loki: {loki_push}", args.verbose)
    status, text = http_post_json(loki_push, payload, timeout=args.timeout, tenant=args.tenant)
    if 200 <= status < 300:
        # brief guidance so you know what to query
        try:
            first = payload["streams"][0]
            log(f"OK {status}. Example labels: {first['stream']}", True)
        except Exception:
            pass
    else:
        log(f"ERROR: Loki status {status}: {text}", True)
        sys.exit(1)


if __name__ == "__main__":
    main()
