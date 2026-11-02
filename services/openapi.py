"""Build the subscriber-facing RapidAPI document without private auth fields."""
from copy import deepcopy


def rapidapi_schema(internal_schema):
    schema = deepcopy(internal_schema)
    schema['openapi'] = '3.0.3'
    schema['servers'] = [{'url': 'https://ydataquote.vercel.app'}]
    schema['info']['description'] = (
        'Latest available stock prices and historical daily close prices for 1–20 symbols. '
        'Choose a period or start/end dates for history. Results are timestamped JSON. '
        'Historical interval is 1d only. Prices may be delayed; coverage is not guaranteed. '
        'Authenticate using your RapidAPI subscription key through the RapidAPI gateway.')
    schema.pop('security', None)
    schema['components'].pop('securitySchemes', None)
    schema['paths'] = {path: item for path, item in schema['paths'].items()
                       if path in ('/v1/quotes', '/v1/history')}
    for item in schema['paths'].values():
        for operation in item.values():
            if isinstance(operation, dict):
                operation.pop('security', None)
    # FastAPI emits OpenAPI 3.1 nullable unions; the import file targets 3.0.
    def normalize(value):
        if isinstance(value, dict):
            variants = value.get('anyOf', [])
            nonnull = [v for v in variants if v != {'type': 'null'}]
            if len(nonnull) == 1 and len(nonnull) != len(variants):
                value.pop('anyOf')
                value.update(nonnull[0])
                value['nullable'] = True
            for child in value.values():
                normalize(child)
        elif isinstance(value, list):
            for child in value:
                normalize(child)
    normalize(schema)
    return schema
