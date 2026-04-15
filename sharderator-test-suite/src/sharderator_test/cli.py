"""CLI entry point for the Sharderator test suite."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import click

from sharderator_test.connection import load_env, make_client

# Inject .env into os.environ early so Click's envvar= resolution picks it up
_env = load_env()
for _k, _v in _env.items():
    os.environ.setdefault(_k, _v)


# Common options shared across commands
def _common_options(fn):
    fn = click.option("--cloud-id", envvar="CLOUD_ID", help="Elastic Cloud deployment ID")(fn)
    fn = click.option("--api-key", envvar="ES_API_KEY", help="API key")(fn)
    return fn


@click.group()
def main():
    """Sharderator end-to-end test suite.

    Creates realistic frozen-tier shard pressure on an Elastic Cloud trial
    for validating Sharderator shrink mode and merge mode.
    """
    pass


@main.command()
@_common_options
def setup(cloud_id, api_key):
    """Configure the cluster: ILM policies, index templates, speed up ILM poll."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    client = make_client(cloud_id, api_key)
    from sharderator_test.fixtures import setup_cluster
    setup_cluster(client, console)


@main.command("ingest-oversharded")
@_common_options
@click.option("--docs-per-stream", default=500_000, show_default=True,
              help="Documents per data stream (triggers multiple rollovers)")
@click.option("--batch-size", default=5_000, show_default=True)
def ingest_oversharded(cloud_id, api_key, docs_per_stream, batch_size):
    """Ingest data into over-sharded data streams (Scenario A)."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    client = make_client(cloud_id, api_key)
    from sharderator_test.ingest import ingest_oversharded as _ingest
    _ingest(client, docs_per_stream=docs_per_stream, batch_size=batch_size, console=console)


@main.command("ingest-daily")
@_common_options
@click.option("--days", default=90, show_default=True,
              help="Number of days of daily indices to create")
@click.option("--docs-per-index", default=10_000, show_default=True,
              help="Documents per daily index")
@click.option("--batch-size", default=5_000, show_default=True)
def ingest_daily(cloud_id, api_key, days, docs_per_index, batch_size):
    """Create daily index explosion (Scenario B): N days × 40 metric types."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    client = make_client(cloud_id, api_key)
    from sharderator_test.ingest import ingest_daily as _ingest
    _ingest(client, days=days, docs_per_index=docs_per_index, batch_size=batch_size, console=console)


@main.command()
@_common_options
@click.option("--watch", is_flag=True, help="Poll continuously until all indices are frozen")
@click.option("--interval", default=30, show_default=True, help="Poll interval in seconds (--watch mode)")
def status(cloud_id, api_key, watch, interval):
    """Show cluster frozen shard budget and ILM progress."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    client = make_client(cloud_id, api_key)
    from sharderator_test.status import print_status, watch_ilm

    if watch:
        watch_ilm(client, interval=interval, console=console)
    else:
        print_status(client, console=console)


@main.command()
@_common_options
@click.option("--phase", default="all",
              type=click.Choice(["all", "read-only", "dry-run", "shrink", "merge",
                                 "crash-recovery", "export"]),
              show_default=True, help="Validation phase to run")
@click.option("--output-dir", default=".", show_default=True,
              help="Directory for exported files")
def validate(cloud_id, api_key, phase, output_dir):
    """Run validation phases against the test cluster."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    if not cloud_id or not api_key:
        msg = (
            "Credentials required. Pass --cloud-id and --api-key, "
            "or set CLOUD_ID and ES_API_KEY environment variables, "
            "or add them to .env"
        )
        if console:
            console.print(f"[red]Error:[/red] {msg}")
        else:
            print(f"Error: {msg}")
        raise SystemExit(1)

    client = make_client(cloud_id, api_key)
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    from sharderator_test.validate import run_phase
    passed = run_phase(phase, client, cloud_id, api_key, out_dir, console)

    if not passed:
        raise SystemExit(1)


@main.command()
@_common_options
@click.option("--dry-run", is_flag=True, help="Show what would be deleted without deleting")
def cleanup(cloud_id, api_key, dry_run):
    """Remove all test artifacts from the cluster."""
    try:
        from rich.console import Console
        console = Console()
    except ImportError:
        console = None

    if not dry_run:
        click.confirm(
            "This will delete all test indices, ILM policies, and templates. Continue?",
            abort=True,
        )

    client = make_client(cloud_id, api_key)
    from sharderator_test.fixtures import cleanup_cluster
    cleanup_cluster(client, dry_run=dry_run, console=console)


@main.command("create-api-key")
@click.option("--cloud-id", envvar="CLOUD_ID", required=True)
@click.option("--username", prompt=True)
@click.option("--password", prompt=True, hide_input=True)
def create_api_key(cloud_id, username, password):
    """Create a Sharderator test API key with full cluster access."""
    from elasticsearch import Elasticsearch
    client = Elasticsearch(
        cloud_id=cloud_id,
        basic_auth=(username, password),
        request_timeout=30,
    )
    result = client.security.create_api_key(
        body={
            "name": "sharderator-test",
            "role_descriptors": {
                "all_access": {
                    "cluster": ["all"],
                    "index": [{"names": ["*"], "privileges": ["all"]}],
                }
            },
        }
    )
    print(f"\nAPI Key created:")
    print(f"  ID:      {result['id']}")
    print(f"  Name:    {result['name']}")
    print(f"  API Key: {result['api_key']}")
    print(f"\nEncoded (use this as --api-key):")
    import base64
    encoded = base64.b64encode(f"{result['id']}:{result['api_key']}".encode()).decode()
    print(f"  {encoded}")
    print(f"\nAdd to .env:")
    print(f"  ES_API_KEY={encoded}")


if __name__ == "__main__":
    main()
