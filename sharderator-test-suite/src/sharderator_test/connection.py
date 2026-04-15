"""Elasticsearch connection helper for the test suite."""

from __future__ import annotations

import os
from pathlib import Path

from elasticsearch import Elasticsearch


def load_env() -> dict[str, str]:
    """Load .env file from the project root if it exists."""
    env_file = Path(__file__).parent.parent.parent / ".env"
    env: dict[str, str] = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip()
    return env


def make_client(cloud_id: str | None = None, api_key: str | None = None) -> Elasticsearch:
    """Create an Elasticsearch client from args or environment."""
    env = load_env()
    cloud_id = cloud_id or os.environ.get("CLOUD_ID") or env.get("CLOUD_ID")
    api_key = api_key or os.environ.get("ES_API_KEY") or env.get("ES_API_KEY")

    if not cloud_id:
        raise ValueError(
            "No CLOUD_ID provided. Pass --cloud-id, set CLOUD_ID env var, or add to .env"
        )
    if not api_key:
        raise ValueError(
            "No API key provided. Pass --api-key, set ES_API_KEY env var, or add to .env"
        )

    return Elasticsearch(cloud_id=cloud_id, api_key=api_key, request_timeout=120)
