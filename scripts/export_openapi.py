"""Run from the repository root: python -m scripts.export_openapi"""
import json
from pathlib import Path

from app import app
from services.openapi import rapidapi_schema


if __name__ == '__main__':
    destination = Path(__file__).resolve().parents[1] / 'openapi.json'
    destination.write_text(json.dumps(rapidapi_schema(app.openapi()), indent=2) + '\n')
    print(destination)
