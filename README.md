# Y Fin Latest Quote

On-demand latest prices and historical **close-price series**, using the Yahoo
Finance Spark endpoint through Decodo. Based on the attached
`decodo_batched_prices_fixed_colab.py`; its original is retained under `reference/`.
No public proxy pool, OHLCV, database, scheduled fetches, or hourly quotas.

## Request contract

- Two separate endpoints: `GET /v1/quotes` and `GET /v1/history`.
- `symbols`: required comma-separated list, 1–20 entries, normalized to uppercase.
  Duplicates are removed. More than 20 supplied entries are rejected.
- One authenticated, validated and configured data request makes **one Decodo
  POST containing one multi-symbol Yahoo Spark URL**, including on upstream
  failure. There are no retries, redirects, per-symbol requests, cookie/crumb
  calls, or fallback endpoints. Validation/auth/configuration failures make zero
  calls. Health and documentation make zero calls.
- This counts client POST attempts, not guaranteed billable credits. Decodo's
  internal retries/accounting and upstream availability are outside this API.
- Every distinct requested symbol has a result, including omitted/invalid symbols.
  HTTP 200 means the response was processed, not that every symbol succeeded.

### Latest prices

```http
GET /v1/quotes?symbols=AAPL,MSFT
```

Requests `range=1d&interval=1m&includePrePost=true`. Selects the newest valid
regular/pre/post market metadata price or one-minute candle close. Results include
`price`, `price_time_utc`, `price_age_seconds`, `currency`, and `any_error`.
Prices may be delayed or from a previous session. Candle timestamps
identify an interval, not necessarily the precise trade time.

### Historical close prices

```http
GET /v1/history?symbols=AAPL,MSFT&period=5y&interval=1d
GET /v1/history?symbols=AAPL,MSFT&start=2023-01-01&end=2026-11-01&interval=1d
```

| Parameter | Meaning |
|---|---|
| `symbols` | Required; maximum 20 comma-separated entries |
| `period` | `1d`, `5d`, `1mo`, `3mo`, `6mo`, `1y`, `2y`, `5y`, `10y`, `ytd`, `max` |
| `start`, `end` | Supply both instead of period; UTC dates, start inclusive and end exclusive |
| `interval` | `1d` only (daily closes); default `1d`. Other intervals return HTTP 422 before any Decodo call. |

Date mode forwards Unix `period1`/`period2` to Spark, then filters returned points
to `[start,end)`. Period mode forwards `range`. No automatic date expansion,
range splitting, resampling or interval substitution. Yahoo may reject a
combination or return less history than requested. **Live custom-date behavior
and long-period coverage must be confirmed using your Decodo account before
advertising them.** The code does not assert complete coverage; each history
result has `coverage_verified: false`. Returned bars retain Yahoo close values
without additional adjustment. The latest bar may still be forming.

Each history result contains `data` objects with `timestamp` (Unix seconds),
`time_utc`, and `close`. Missing/invalid values are omitted with a `partial` status
and warnings; no data yields status `error`. `any_error` is null on success or
an explanatory message on failure. No open/high/low/volume fields are returned.

Example response shape (illustrative data):

```json
{
  "fetched_at_utc": "2026-11-02T00:00:00+00:00",
  "data_calls": 1,
  "requested_symbols": 1,
  "successful_symbols": 1,
  "results": [{
    "symbol": "AAPL",
    "status": "ok",
    "currency": "USD",
    "exchange_timezone": "America/New_York",
    "data": [{"timestamp": 1704067200, "time_utc": "2024-01-01T00:00:00+00:00", "close": 100}],
    "coverage_verified": false,
    "warnings": [],
    "any_error": null
  }]
}
```

`data_calls` counts the single upstream client request attempt for the processed
batch; it is not a billable-credit count. Each result uses `any_error` for its
error message; HTTP-level failures continue to use the `detail` field.

HTTP errors: `401` invalid credentials, `422` invalid input, `503` missing server
configuration, `502` upstream HTTP/timeout/format failure. Upstream failures never
trigger another Decodo request. Raw upstream bodies and credentials are not
included in errors. Data responses use `Cache-Control: no-store`.

## Run locally

Python 3.12 recommended. Install dependencies, set keys, and run:

```bash
pip install -r requirements.txt
export DECODO_API_KEY='your-decodo-key'
export API_KEY='your-private-test-key'
uvicorn app:app --reload
```

`.env.example` documents variables; the app does not automatically load `.env`.

```bash
curl 'http://127.0.0.1:8000/v1/quotes?symbols=AAPL,MSFT' -H "X-API-Key: $API_KEY"
curl 'http://127.0.0.1:8000/v1/history?symbols=AAPL,MSFT&period=1y&interval=1d' -H "X-API-Key: $API_KEY"
```

URL-encode special symbol characters: `M&M.NS` becomes `M%26M.NS` in query URLs.
No user-supplied target URL, Decodo key, or proxy credentials are accepted.

## Deploy on Vercel

1. Import `mukundan1989/ydataquote` into Vercel with repository root as root directory.
2. Use the detected FastAPI framework (`app.py` exports `app`). Install dependencies
   from `requirements.txt`; no frontend build/output directory is needed.
3. Add `DECODO_API_KEY` and a private `API_KEY` in Vercel environment variables.
   Add `RAPIDAPI_PROXY_SECRET` when connecting RapidAPI. Redeploy after changes.
4. The included `vercel.json` sets a 60-second function duration. The Decodo client
   has a 10-second connection timeout and 45-second read timeout; slow requests
   can still be terminated by Vercel or the gateway. There are no retries.
5. Visit `/health`, `/docs`, and `/openapi.json`. Health is process-only and never
   tests Yahoo. Test both data endpoints and date/period combinations live.
6. Ensure the RapidAPI gateway can reach the production API through any Vercel
   deployment protection settings. Application-level authentication remains required.

Official deployment documentation: https://vercel.com/docs/frameworks/backend/fastapi

## Connect RapidAPI

1. Add an API in the Provider Dashboard and import `openapi.json` from this repo
   (or download the live `/rapidapi-openapi.json`). Do not import `/openapi.json`,
   which describes private direct testing and retains its authentication controls.
2. Set the deployed production Vercel URL as the API's base URL.
3. Configure the two GET endpoints and describe the 20-symbol limit and close-only
   response. The specification lists the accepted query parameters.
4. Copy the proxy secret from RapidAPI's Security settings to the Vercel
   `RAPIDAPI_PROXY_SECRET` environment variable and redeploy.
5. Subscribers use their RapidAPI key; RapidAPI adds `X-RapidAPI-Proxy-Secret` when
   forwarding. The import specification excludes both private authentication headers;
   subscribers do not enter your `API_KEY`, proxy secret, or Decodo key. Keep the
   default RapidAPI gateway authentication enabled in the listing. To disallow
   direct testing, delete `API_KEY` after setup and redeploy.
6. Test quotes, period history, custom dates, and partial failures through the
   gateway before publishing. Configure subscription pricing separately; this app
   implements no hourly rate limit.

Data endpoints require at least one configured credential. With both configured,
a matching proxy secret OR private test key authorizes the request. Public docs
and health do not authorize paid upstream fetches. Keep all real keys out of Git.

## Endpoint names

The API title is **Y Fin Latest Quote**. Display summaries and stable operation
IDs are explicit, so imports do not use generated function/path names:

| Path | Display summary | Operation ID |
|---|---|---|
| `/v1/quotes` | Latest Stock Quotes | `LatestStockQuotes` |
| `/v1/history` | Historical Daily Close Prices | `HistoricalDailyClosePrices` |
| `/health` | Health Check | `HealthCheck` |

Data endpoints are grouped under **Stock Prices**. The repository name and
existing Vercel URL stay the same. Reimport the updated specification in RapidAPI
for the new metadata; existing listing names may need to be edited in its dashboard.

## API specifications

- `/docs` and `/openapi.json`: direct testing documentation with separate named
  authentication schemes for `X-API-Key` and the proxy secret. Either configured
  credential is accepted; backend authorization is always enforced.
- `/rapidapi-openapi.json` and repository `openapi.json`: subscriber-facing
  OpenAPI 3.0 document with only the two data endpoints, the deployed base URL,
  and no private authentication schemes. RapidAPI supplies its gateway auth.
- Regenerate the checked-in import file after endpoint changes:
  `python -m scripts.export_openapi`. The live import document uses the same
  builder, so private credentials are not reintroduced during regeneration.
- If you already imported an earlier file, replace/update its definition in
  RapidAPI and remove any leftover private header requirements there.

## Verification

```bash
pip install -r requirements-dev.txt
python -m pytest -q
```

Tests mock Decodo: verify 1/20-symbol call counts, date forwarding/filtering,
invalid requests making zero calls, missing/mismatched symbols, parsing, auth,
secret redaction, and no retries on upstream failures. They consume no credits.
Live upstream access, custom-date support, data completeness, gateway timeouts,
and billing require deployment/account testing; mock tests do not establish them.

The original Colab reference is not the deployed entrypoint and is excluded from
Vercel's function bundle. No credentials are committed.
