"""Spark parsing adapted from decodo_batched_prices_fixed_colab.py."""
import json
import math
import time
from datetime import datetime, timezone
from html.parser import HTMLParser


class UpstreamError(Exception):
    pass


def utc(stamp=None):
    return datetime.fromtimestamp(time.time() if stamp is None else stamp, timezone.utc).isoformat()


def finite_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def valid_pair(price, stamp, now):
    return finite_number(price) and price > 0 and finite_number(stamp) and 0 < stamp <= now + 300


def content_json(content):
    if isinstance(content, dict):
        return content
    if isinstance(content, str):
        try:
            return json.loads(content)
        except ValueError:
            class PreParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.depth, self.parts = 0, []

                def handle_starttag(self, tag, attrs):
                    if tag == 'pre':
                        self.depth += 1

                def handle_endtag(self, tag):
                    if tag == 'pre':
                        self.depth = max(0, self.depth - 1)

                def handle_data(self, data):
                    if self.depth:
                        self.parts.append(data)
            parser = PreParser()
            parser.feed(content)
            try:
                return json.loads(''.join(parser.parts))
            except ValueError:
                pass
    raise UpstreamError('Yahoo returned invalid JSON content')


def spark_items(payload):
    spark = payload.get('spark') if isinstance(payload, dict) else None
    if not isinstance(spark, dict) or spark.get('error') or not isinstance(spark.get('result'), list):
        raise UpstreamError('Yahoo returned an invalid or rejected Spark response')
    return {str(x.get('symbol', '')).upper(): x for x in spark['result'] if isinstance(x, dict)}


def responses_for(item, symbol):
    responses = item.get('response', []) if isinstance(item, dict) else []
    if not isinstance(responses, list):
        return []
    return [r for r in responses if isinstance(r, dict) and isinstance(r.get('meta'), dict)
            and str(r['meta'].get('symbol', symbol)).upper() == symbol]


def close_pairs(response):
    timestamps = response.get('timestamp')
    indicators = response.get('indicators')
    quotes = indicators.get('quote') if isinstance(indicators, dict) else None
    closes = quotes[0].get('close') if isinstance(quotes, list) and quotes and isinstance(quotes[0], dict) else None
    if not isinstance(timestamps, list) or not isinstance(closes, list):
        return [], True
    return list(zip(timestamps, closes)), len(timestamps) != len(closes)


def parse_quotes(payload, symbols, now=None):
    now = time.time() if now is None else now
    items, results = spark_items(payload), []
    for symbol in symbols:
        candidates = []
        for response in responses_for(items.get(symbol), symbol):
            meta = response['meta']
            for session in ('regular', 'pre', 'post'):
                price, stamp = meta.get(session + 'MarketPrice'), meta.get(session + 'MarketTime')
                if valid_pair(price, stamp, now):
                    candidates.append((stamp, 1, price, session + '_market_quote', session, meta))
            pairs, _ = close_pairs(response)
            for stamp, price in pairs:
                if valid_pair(price, stamp, now):
                    candidates.append((stamp, 0, price, 'one_minute_close', 'unknown', meta))
        if not candidates:
            results.append(dict(symbol=symbol, status='error', error='No valid matching price returned'))
            continue
        stamp, _, price, source, session, meta = max(candidates, key=lambda x: (x[0], x[1]))
        results.append(dict(symbol=symbol, status='ok', price=price, price_time_utc=utc(stamp),
                            price_age_seconds=round(now-stamp), price_source=source,
                            market_session=session, currency=meta.get('currency'),
                            exchange_delay_minutes=meta.get('exchangeDataDelayedBy')))
    return results


def parse_history(payload, symbols, start=None, end=None, now=None):
    now = time.time() if now is None else now
    items, results = spark_items(payload), []
    for symbol in symbols:
        bars, invalid, meta = {}, False, {}
        for response in responses_for(items.get(symbol), symbol):
            meta = response['meta']
            pairs, mismatch = close_pairs(response)
            invalid |= mismatch
            for stamp, close in pairs:
                if not valid_pair(close, stamp, now):
                    invalid = True
                    continue
                if (start is not None and stamp < start) or (end is not None and stamp >= end):
                    continue
                bars[stamp] = close
        points = [dict(timestamp=int(t), time_utc=utc(t), close=bars[t]) for t in sorted(bars)]
        results.append(dict(symbol=symbol, status=('partial' if invalid else 'ok') if points else 'error',
                            currency=meta.get('currency'), exchange_timezone=meta.get('exchangeTimezoneName'),
                            data=points, coverage_verified=False,
                            warnings=['Invalid or unpaired values were omitted'] if invalid else [],
                            error=None if points else 'No valid matching close prices in the requested window'))
    return results
