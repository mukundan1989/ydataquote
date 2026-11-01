# YDataQuote

On-demand Yahoo Finance API for up to 10 symbols per call. Fetch intraday bars,
historical bars, or regular-market price snapshots through a shared HTTP/SOCKS5
proxy pool. **No hourly quotas. No stock database or bulk prefetching.**

This is a first runnable implementation for testing, not a proven production feed.
The included public proxy candidates passed earlier user-run tests; they are not
assumed to work now. A successful API connection does not guarantee Yahoo data.

## Start locally

Requires Python 3.12+.

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
```

Windows activation: `.venv\Scripts\activate`.
Open http://127.0.0.1:8000/docs for interactive API testing.
The default uses `data/proxies.sample.txt`, containing 32 credential-free public
candidates: 16 passed 4/4 and 16 passed 3/4 requests in the October 28, 2026 test.
These measurements are historical and are not a reliability guarantee.

Or start with Docker:

```bash
docker compose up --build
```

The Compose port binds to localhost. Set `API_KEY` and deliberately configure
network exposure/TLS before making this available to other users. Authentication
is optional for local testing; it does not introduce an hourly quota.

## Intraday

```bash
curl -X POST http://127.0.0.1:8000/v1/history \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["AAPL","MSFT"],"interval":"5m","range":"5d"}'
```

## Historical

```bash
curl -X POST http://127.0.0.1:8000/v1/history \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["AAPL","MSFT"],"interval":"1d","start":"2023-01-01","end":"2026-11-02"}'
```

`end` is **exclusive**. Date-only and timezone-free inputs are interpreted as UTC;
use ISO-8601 offsets when you need a specific local-time boundary. Supply either
`range` or both `start` and `end`. Without either, range defaults to `1mo`.

Intervals: `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`,
`1mo`, `3mo`. Range: `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`,
`ytd`, `max`. Optional `prepost` defaults to false.

Yahoo imposes interval/date availability constraints. The API forwards validly
formed requests and reports Yahoo rejections; it does not promise unlimited
intraday history or silently substitute a different interval. There is no range
splitting in this version.

## Price snapshots

```bash
curl -X POST http://127.0.0.1:8000/v1/quotes \
  -H 'Content-Type: application/json' \
  -d '{"symbols":["AAPL","MSFT","TCS.NS"]}'
```

Uses Yahoo chart metadata (`regularMarketPrice`, `regularMarketTime`). This is
not streaming data or bid/ask; the price may be delayed or from the last session.

## Response contract

A processed batch returns HTTP 200 with one result per distinct symbol:

- `status: ok`: usable data returned.
- `status: partial`: invalid/duplicate bars were omitted; inspect `warnings`.
- `status: error`: `error.code` and a sanitized explanation; no fabricated bars.

Every result has `attempts` and `elapsed_seconds`. Successes include `data` and
an opaque `proxy_id`. History includes UTC bar timestamps, OHLCV and nullable
`adj_close`. Yahoo values are retained as supplied, not additionally adjusted.
`coverage_verified: false` explicitly means missing exchange sessions/bars are
not detected. The most recent candle may still be forming.

The summary counts `ok`, `partial`, and `error`. **HTTP 200 alone does not mean all
symbols succeeded.** Invalid inputs return 422, failed API authentication 401,
and a full in-memory work queue/no configured proxies 503.

## Scheduling and failure handling

- Default: up to 16 simultaneous upstream attempts across different proxy hosts.
- One active request per proxy hostname, across all users and all ports on that
  hostname; at least one second after completion before that host is used again.
- Up to three attempts per symbol, each on a different hostname.
- Connection failures temporarily quarantine a route (default 30 seconds).
- Proxy CONNECT 429 and Yahoo HTTP 429 both trigger a pool-wide cooldown, at
  least 60 seconds and respecting a supplied Retry-After. Already-started work
  may finish. New attempts do not rotate around that cooldown.
- Each symbol has a 45-second deadline, including time waiting for a route.
- At most 200 outstanding symbols, including queued and active work. Excess
  batches get 503, not an hourly quota. Admission is released on completion.
- No direct Yahoo fallback, proxy URLs supplied by API callers, persistent price
  store, scheduled scraping, or hourly accounting.

**Use exactly one Uvicorn worker and one instance for this version.** Pool leases,
spacing, queue capacity, and cooldowns are process-local. Multiple processes or
replicas would need shared coordination (for example Redis), which is not
implemented. Different hostnames may still share a provider's account limits or
an external exit IP; the service cannot infer that automatically.

## Configure proxies and settings

Create a private `proxies.txt` (gitignored), one URL per line:

```text
http://HOST:PORT
http://USERNAME:PASSWORD@HOST:PORT
socks5://HOST:PORT
```

URL-encode special characters in credentials. Bare IP:port is treated as HTTP.
Comments beginning with `#` are allowed. Duplicate endpoints are removed. Private
literal IPs are rejected. Proxy configuration is trusted server-side input.
No credentials are printed in API results or proxy statistics.

```bash
export PROXY_FILE=proxies.txt
export API_KEY='choose-your-own-key'
uvicorn app.main:app --host 127.0.0.1 --port 8000 --workers 1
```

Add `-H 'X-API-Key: your-key'` to API calls when configured. `.env.example` is a
reference: local Uvicorn does not automatically load it. Restart after changing
proxy configuration. For Docker, mount your file read-only and set `PROXY_FILE`
to the container path. Never commit paid proxy credentials or API keys.

| Environment variable | Default |
|---|---:|
| `PROXY_FILE` | `data/proxies.sample.txt` |
| `API_KEY` | Empty (local auth disabled) |
| `MAX_CONCURRENT_FETCHES` | 16 |
| `PROXY_GAP_SECONDS` | 1 (minimum 1) |
| `MAX_ATTEMPTS` | 3 |
| `REQUEST_DEADLINE_SECONDS` | 45 |
| `PROXY_COOLDOWN_SECONDS` | 30 |
| `MAX_PENDING_SYMBOLS` | 200 |

`GET /health` checks API process health, not live Yahoo availability.
`GET /v1/proxies` shows opaque IDs, attempts, successes, failures, latency, and
cooldowns. `verified_in_this_process` means at least one successful fetch since
startup, not guaranteed current health. Statistics reset when the process restarts.
Both data endpoints and proxy statistics use the optional API key. Health/docs
remain public. No browser CORS access is enabled by default.

## Check it

```bash
pip install -r requirements-dev.txt
python -m pytest -q
python scripts/smoke.py --url http://127.0.0.1:8000 --symbols AAPL,MSFT --interval 5m --range 5d
```

Tests mock Yahoo and proxy failures; they cover validation, intraday timestamps,
per-host spacing, concurrency limits, distinct-host retries, cancellation/deadline
cleanup, global 429 cooldown, queue admission, auth, and secret redaction. GitHub
Actions runs them on pushes and PRs without live proxy requests or secrets.
The smoke command uses real upstream requests and exits 2 if any symbol fails.

Free proxies and Yahoo's unofficial chart endpoint can fail or change. Successful
unit tests are not a live availability or capacity benchmark. Before serving
other users, run realistic mixed intraday/history load tests on the deployment
host and verify applicable Yahoo/exchange data-use permissions.
