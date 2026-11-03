"""Vercel FastAPI entrypoint for on-demand Spark quotes and close history."""
import os
import re
import secrets
from datetime import date, datetime, timezone
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, BeforeValidator, model_serializer

from services.decodo import ConfigurationError, fetch_spark
from services.spark import UpstreamError, parse_history, parse_quotes, utc
from services.openapi import rapidapi_schema

app = FastAPI(title='Y Fin Latest Quote', version='1.0.2', description=(
    'Latest available prices and historical close series for 1–20 symbols through Yahoo Spark. '
    'One admitted data request makes one Decodo POST; no retries, OHLCV or hourly quotas. '
    'Prices may be delayed. Upstream availability and historical coverage are not guaranteed.'))


AVAILABLE_PERIODS = ('1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', '10y', 'ytd', 'max')
PERIOD_DESCRIPTION = (
    'Available options: ' + ', '.join(AVAILABLE_PERIODS) + '. '
    'Keep start/end dates empty if you want to return values for this period. '
    'Use either Period or Date Range. Ignored when both dates are provided. '
    'The form is prefilled with 1y; if period and dates are empty, 5d is used.')
DATE_RANGE_DESCRIPTION = (
    'If both date range and period boxes are filled, date range is considered. '
    'Use either period or date range.')
InputDate = Annotated[date | None, BeforeValidator(
    lambda value: None if isinstance(value, str) and not value.strip() else value)]


class PriceResult(BaseModel):
    @model_serializer(mode='wrap')
    def serialize_result(self, handler):
        result = handler(self)
        if result.get('any_error') is None:
            result.pop('any_error', None)
        return result


class QuoteResult(PriceResult):
    symbol: str
    price: float | None = None
    price_time_utc: str | None = None
    currency: str | None = None
    any_error: str | None = None


class ClosePoint(BaseModel):
    timestamp: int
    time_utc: str
    close: float


class HistoryResult(PriceResult):
    symbol: str
    currency: str | None = None
    exchange_timezone: str | None = None
    data: list[ClosePoint]
    coverage_verified: bool
    warnings: list[str]
    any_error: str | None = None


class BatchResponse(BaseModel):
    fetched_at_utc: str
    data_calls: int
    requested_symbols: int
    successful_symbols: int


class QuoteResponse(BatchResponse):
    results: list[QuoteResult]


class HistoryResponse(BatchResponse):
    results: list[HistoryResult]


proxy_header = APIKeyHeader(name='X-RapidAPI-Proxy-Secret', scheme_name='RapidAPIProxySecret', auto_error=False)
test_header = APIKeyHeader(name='X-API-Key', scheme_name='DirectTestKey', auto_error=False)


def authorize(proxy: str | None = Depends(proxy_header), key: str | None = Depends(test_header)):
    expected_proxy = os.getenv('RAPIDAPI_PROXY_SECRET', '').strip()
    expected_key = os.getenv('API_KEY', '').strip()
    if expected_proxy and secrets.compare_digest(proxy or '', expected_proxy):
        return
    if expected_key and secrets.compare_digest(key or '', expected_key):
        return
    if expected_proxy or expected_key:
        raise HTTPException(401, 'Invalid API credentials')
    # Never accidentally publish a paid upstream behind an unprotected endpoint.
    raise HTTPException(503, 'Configure API_KEY or RAPIDAPI_PROXY_SECRET before using data endpoints')


def parse_symbols(symbols: Annotated[str, Query(min_length=1, max_length=1024,
                  description='Comma-separated symbols. Add up to 20 symbols. Example: MSFT,TSLA,INFY.')]):
    entries = [s.strip().upper() for s in symbols.split(',')]
    if not 1 <= len(entries) <= 20 or any(not re.fullmatch(r'[A-Z0-9^][A-Z0-9.^=\-&]{0,39}', s) for s in entries):
        raise HTTPException(422, 'Supply 1–20 comma-separated Yahoo symbols, without empty entries')
    return list(dict.fromkeys(entries))


def run(symbols, query, history=False, start=None, end=None):
    try:
        payload = fetch_spark(symbols, query)
        results = parse_history(payload, symbols, start, end) if history else parse_quotes(payload, symbols)
    except ConfigurationError as exc:
        raise HTTPException(503, str(exc)) from None
    except UpstreamError as exc:
        raise HTTPException(502, str(exc)) from None
    return dict(fetched_at_utc=utc(), data_calls=1, requested_symbols=len(symbols),
                successful_symbols=sum(r['status'] in ('ok', 'partial') for r in results), results=results)


@app.middleware('http')
async def no_cache(request, call_next):
    response = await call_next(request)
    response.headers['Cache-Control'] = 'no-store'
    return response


@app.get('/health', tags=['Health'], summary='Health Check', operation_id='HealthCheck')
def health():
    """Process health only; makes no Decodo call."""
    return {'status': 'ok'}


@app.get('/v1/quotes', response_model=QuoteResponse, dependencies=[Depends(authorize)],
         tags=['Stock Prices'], summary='Latest Stock Quotes', operation_id='LatestStockQuotes')
def quotes(symbols: list[str] = Depends(parse_symbols)):
    """Latest quotes with timestamp."""
    return run(symbols, dict(range='1d', interval='1m', includePrePost='true'))


@app.get('/v1/history', response_model=HistoryResponse, dependencies=[Depends(authorize)],
         tags=['Stock Prices'], summary='Historical Daily Close Prices', operation_id='HistoricalDailyClosePrices')
def history(request: Request, symbols: list[str] = Depends(parse_symbols), *,
            interval: Annotated[str, Query(
                description='Available option: 1d (daily closes only).',
                json_schema_extra={'default': '1d'})],
            period: Annotated[str, Query(description=PERIOD_DESCRIPTION)] = '1y',
            start: Annotated[InputDate, Query(description=(
                'Inclusive UTC date (YYYY-MM-DD). ' + DATE_RANGE_DESCRIPTION))] = None,
            end: Annotated[InputDate, Query(description=(
                'Exclusive UTC date (YYYY-MM-DD). ' + DATE_RANGE_DESCRIPTION))] = None):
    """Fetch historical daily close prices. Choose either a period or a date range.

    A complete date range overrides period. Empty period and dates use 5d.
    You can fetch up to 20 symbols. Interval must be 1d.
    """
    if interval != '1d':
        raise HTTPException(422, 'Only interval 1d is supported')
    if (start is None) != (end is None):
        raise HTTPException(422, 'Supply both start and end dates')
    query = dict(interval='1d', includePrePost='false')
    start_ts = end_ts = None
    if start is not None and end is not None:
        if start < date(1970, 1, 1) or start >= end:
            raise HTTPException(422, 'Dates must satisfy 1970-01-01 <= start < end')
        if start > datetime.now(timezone.utc).date():
            raise HTTPException(422, 'Start date cannot be in the future')
        start_ts = int(datetime.combine(start, datetime.min.time(), timezone.utc).timestamp())
        end_ts = int(datetime.combine(end, datetime.min.time(), timezone.utc).timestamp())
        query.update(period1=start_ts, period2=end_ts)
    else:
        # A displayed default is distinct from an omitted query parameter.
        selected_period = (period.strip() if 'period' in request.query_params else '') or '5d'
        if selected_period not in AVAILABLE_PERIODS:
            raise HTTPException(422, 'Available periods: ' + ', '.join(AVAILABLE_PERIODS))
        query['range'] = selected_period
    return run(symbols, query, history=True, start=start_ts, end=end_ts)


@app.get('/rapidapi-openapi.json', include_in_schema=False)
def rapidapi_openapi():
    """Import this public specification into RapidAPI; no upstream calls."""
    return rapidapi_schema(app.openapi())
