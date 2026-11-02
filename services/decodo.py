"""Exactly one client POST, one Yahoo Spark URL; no retries or redirects."""
import json
import os
import time

import requests
from .spark import UpstreamError, content_json

DECODO_ENDPOINT = 'https://data.decodo.com/v1/scrape'
YAHOO_ENDPOINT = 'https://query1.finance.yahoo.com/v7/finance/spark'
MAX_BYTES = 20_000_000


class ConfigurationError(Exception):
    pass


def fetch_spark(symbols, query):
    token = (os.getenv('DECODO_API_KEY') or os.getenv('DECODO_API_TOKEN') or '').strip()
    if token.lower().startswith('bearer '):
        token = token[7:].strip()
    if not token:
        raise ConfigurationError('DECODO_API_KEY is not configured')
    params = dict(symbols=','.join(symbols), includeTimestamps='true', indicators='close', **query)
    url = requests.Request('GET', YAHOO_ENDPOINT, params=params).prepare().url
    body = dict(url=url, proxy_pool='premium', geo='United States', markdown=False)
    started = time.monotonic()
    try:
        with requests.Session() as session:
            session.trust_env = False
            # Requests' default adapters perform zero automatic retries.
            with session.post(DECODO_ENDPOINT, json=body,
                              headers={'Authorization': 'Bearer ' + token, 'Accept': 'application/json'},
                              timeout=(10, 45), stream=True, allow_redirects=False) as response:
                if response.status_code != 200:
                    raise UpstreamError(f'Decodo returned HTTP {response.status_code}')
                data = bytearray()
                for chunk in response.iter_content(65536):
                    data.extend(chunk)
                    if len(data) > MAX_BYTES or time.monotonic()-started > 55:
                        raise UpstreamError('Upstream response exceeded the size or time limit')
        envelope = json.loads(data)
        results = envelope.get('results') if isinstance(envelope, dict) else None
        if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
            raise UpstreamError('Decodo must return exactly one target response')
        target = results[0]
        if str(target.get('status_code')) != '200':
            raise UpstreamError('Yahoo rejected the batched Spark request')
        return content_json(target.get('content'))
    except requests.Timeout:
        raise UpstreamError('Upstream request timed out; no retry was attempted') from None
    except (requests.RequestException, ValueError, TypeError):
        # Never expose exception URLs, tokens, headers or raw upstream bodies.
        raise UpstreamError('Upstream request or response failed; no retry was attempted') from None
