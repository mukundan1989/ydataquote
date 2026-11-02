import json
from urllib.parse import parse_qs, urlparse
from unittest.mock import MagicMock

import pytest
import requests
from fastapi.testclient import TestClient
import app as api
from services import decodo
from services.spark import parse_quotes, parse_history

client = TestClient(api.app)


def payload(symbols=('AAPL', 'MSFT')):
    return {'spark': {'result': [{'symbol': s, 'response': [{
        'meta': {'symbol': s, 'currency': 'USD', 'regularMarketPrice': 110,
                 'regularMarketTime': 1704153660},
        'timestamp': [1704067200, 1704153600, 1704240000],
        'indicators': {'quote': [{'close': [100, 102, 104]}]}
    }]} for s in symbols], 'error': None}}


@pytest.fixture(autouse=True)
def environment(monkeypatch):
    monkeypatch.setenv('API_KEY', 'test-secret')
    monkeypatch.setenv('DECODO_API_KEY', 'decodo-secret')
    monkeypatch.delenv('RAPIDAPI_PROXY_SECRET', raising=False)


@pytest.fixture
def upstream(monkeypatch):
    response = MagicMock()
    response.status_code = 200
    response.iter_content.return_value = [json.dumps({'results': [
        {'status_code': 200, 'content': json.dumps(payload())}]}).encode()]
    response.__enter__.return_value = response
    session = MagicMock()
    session.post.return_value = response
    session.__enter__.return_value = session
    monkeypatch.setattr(decodo.requests, 'Session', lambda: session)
    return session, response


def get(path, **params):
    return client.get(path, params=params, headers={'X-API-Key': 'test-secret'})


@pytest.mark.parametrize('count', [1, 20])
@pytest.mark.parametrize('endpoint', ['/v1/quotes', '/v1/history'])
def test_exactly_one_post_for_batch(upstream, count, endpoint):
    session, _ = upstream
    symbols = ['AAPL'] + [f'TEST{i}' for i in range(count-1)]
    query = {'symbols': ','.join(symbols)}
    if endpoint.endswith('history'):
        query['period'] = '5y'
    response = get(endpoint, **query)
    assert response.status_code == 200
    assert response.json()['decodo_calls'] == 1
    assert len(response.json()['results']) == count
    session.post.assert_called_once()
    kwargs = session.post.call_args.kwargs
    assert kwargs['allow_redirects'] is False
    params = parse_qs(urlparse(kwargs['json']['url']).query)
    assert params['symbols'] == [','.join(symbols)]
    assert params['indicators'] == ['close']
    assert 'decodo-secret' not in response.text


@pytest.mark.parametrize('params', [
    {'symbols': ''}, {'symbols': 'AAPL,'}, {'symbols': ','.join(f'T{i}' for i in range(21))},
    {'symbols': 'AAPL', 'period': '5y', 'start': '2023-01-01'},
    {'symbols': 'AAPL'}, {'symbols': 'AAPL', 'start': '2023-01-01'},
    {'symbols': 'AAPL', 'start': '2024-02-01', 'end': '2023-01-01'},
    {'symbols': 'AAPL', 'period': '5y', 'interval': 'bad'},
    {'symbols': 'https://example.com', 'period': '5y'},
])
def test_invalid_input_makes_zero_posts(upstream, params):
    session, _ = upstream
    assert get('/v1/history', **params).status_code == 422
    session.post.assert_not_called()


def test_dates_forwarded_and_filtered(upstream):
    session, _ = upstream
    response = get('/v1/history', symbols='AAPL', start='2024-01-01', end='2024-01-03')
    assert response.status_code == 200
    assert [p['close'] for p in response.json()['results'][0]['data']] == [100, 102]
    params = parse_qs(urlparse(session.post.call_args.kwargs['json']['url']).query)
    assert params['period1'] == ['1704067200']
    assert params['period2'] == ['1704240000']
    assert 'range' not in params
    session.post.assert_called_once()


@pytest.mark.parametrize('failure', ['timeout', 'http', 'html', 'envelope', 'yahoo'])
def test_no_retry_on_failure(upstream, failure):
    session, response = upstream
    if failure == 'timeout':
        session.post.side_effect = requests.Timeout('decodo-secret')
    elif failure == 'http':
        response.status_code = 429
    else:
        body = {'results': [{'status_code': 200, 'content': '<html>blocked</html>'}]}
        if failure == 'envelope':
            body = {'results': []}
        if failure == 'yahoo':
            body['results'][0]['status_code'] = 429
        response.iter_content.return_value = [json.dumps(body).encode()]
    result = get('/v1/quotes', symbols='AAPL')
    assert result.status_code == 502
    assert 'decodo-secret' not in result.text
    session.post.assert_called_once()


def test_auth_and_configuration(upstream, monkeypatch):
    session, _ = upstream
    assert client.get('/v1/quotes?symbols=AAPL').status_code == 401
    monkeypatch.delenv('API_KEY')
    assert get('/v1/quotes', symbols='AAPL').status_code == 503
    monkeypatch.setenv('RAPIDAPI_PROXY_SECRET', 'proxy-secret')
    assert client.get('/v1/quotes?symbols=AAPL', headers={'X-RapidAPI-Proxy-Secret': 'proxy-secret'}).status_code == 200
    session.post.assert_called_once()


def test_health_docs_no_posts(upstream):
    session, _ = upstream
    for path in ['/health', '/docs', '/openapi.json']:
        assert client.get(path).status_code == 200
    session.post.assert_not_called()


def test_latest_timestamp_wins_and_missing_symbol():
    result = parse_quotes(payload(), ['AAPL', 'MISSING'], now=1704240060)
    assert result[0]['price'] == 104
    assert result[0]['price_source'] == 'one_minute_close'
    assert result[0]['price_age_seconds'] == 60
    assert result[1]['status'] == 'error'


def test_history_omits_invalid_and_never_substitutes_symbols():
    data = payload(('AAPL',))
    data['spark']['result'][0]['response'][0]['indicators']['quote'][0]['close'] = [100, None, 104]
    result = parse_history(data, ['AAPL', 'MSFT'])
    assert result[0]['status'] == 'partial'
    assert len(result[0]['data']) == 2
    assert result[1]['status'] == 'error'
    data['spark']['result'][0]['response'][0]['meta']['symbol'] = 'WRONG'
    assert parse_quotes(data, ['AAPL'])[0]['status'] == 'error'


def test_encoded_symbol_and_deduplication(upstream):
    session, _ = upstream
    response = get('/v1/quotes', symbols='m&m.ns,AAPL,aapl')
    assert response.status_code == 200
    assert response.json()['requested_symbols'] == 2
    params = parse_qs(urlparse(session.post.call_args.kwargs['json']['url']).query)
    assert params['symbols'] == ['M&M.NS,AAPL']


def test_missing_decodo_key_zero_posts(upstream, monkeypatch):
    session, _ = upstream
    monkeypatch.delenv('DECODO_API_KEY')
    monkeypatch.delenv('DECODO_API_TOKEN', raising=False)
    assert get('/v1/quotes', symbols='AAPL').status_code == 503
    session.post.assert_not_called()


@pytest.mark.parametrize('interval', ['1m', '5m', '15m', '30m', '60m', '1h', '1wk', '1mo'])
def test_history_rejects_non_daily_intervals_without_call(upstream, interval):
    session, _ = upstream
    assert get('/v1/history', symbols='AAPL', period='1y', interval=interval).status_code == 422
    session.post.assert_not_called()


@pytest.mark.parametrize('explicit', [False, True])
def test_history_daily_default_and_explicit(upstream, explicit):
    session, _ = upstream
    params = dict(symbols='AAPL', period='1y')
    if explicit:
        params['interval'] = '1d'
    assert get('/v1/history', **params).status_code == 200
    session.post.assert_called_once()
    query = parse_qs(urlparse(session.post.call_args.kwargs['json']['url']).query)
    assert query['interval'] == ['1d']


def test_history_schema_only_offers_daily():
    schema = client.get('/openapi.json').json()
    assert schema['components']['schemas']['Interval']['enum'] == ['1d']



def test_history_has_no_prepost_parameter(upstream):
    session, _ = upstream
    schema = client.get('/openapi.json').json()
    params = schema['paths']['/v1/history']['get']['parameters']
    assert 'prepost' not in [p['name'] for p in params]
    assert get('/v1/history', symbols='AAPL', period='1y').status_code == 200
    query = parse_qs(urlparse(session.post.call_args.kwargs['json']['url']).query)
    assert query['includePrePost'] == ['false']
    session.post.assert_called_once()



def test_rapidapi_spec_is_clean_and_does_not_mutate_private_schema(upstream):
    session, _ = upstream
    public = client.get('/rapidapi-openapi.json').json()
    assert public['openapi'] == '3.0.3'
    assert set(public['paths']) == {'/v1/quotes', '/v1/history'}
    assert public['servers'] == [{'url': 'https://ydataquote.vercel.app'}]
    assert 'securitySchemes' not in public['components']
    assert 'security' not in public
    for path in public['paths'].values():
        assert 'security' not in path['get']
    assert 'X-API-Key' not in json.dumps(public)
    assert 'X-RapidAPI-Proxy-Secret' not in json.dumps(public)
    internal = client.get('/openapi.json').json()
    schemes = internal['components']['securitySchemes']
    assert schemes['DirectTestKey']['name'] == 'X-API-Key'
    assert schemes['RapidAPIProxySecret']['name'] == 'X-RapidAPI-Proxy-Secret'
    assert client.get('/v1/quotes?symbols=AAPL').status_code == 401
    session.post.assert_not_called()


def test_checked_in_rapidapi_spec_matches_live_builder():
    from pathlib import Path
    saved = json.loads((Path(__file__).resolve().parents[1] / 'openapi.json').read_text())
    assert saved == client.get('/rapidapi-openapi.json').json()
