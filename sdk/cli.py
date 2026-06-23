"""
LogFoundry CLI — Command-line interface for querying and tailing logs.

Usage:
    logfoundry tail --service payments-api --level ERROR
    logfoundry tail --search "connection refused" --since 1h
    logfoundry stats --service payments-api

Implementation:
  - tail: polls /query every 2 seconds, prints new logs, tracks last timestamp
  - stats: fetches /metrics and displays formatted counters
  - Uses click for argument parsing and colorizes output by log level
"""

from __future__ import annotations

import json
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone
from typing import Optional

try:
    import click
except ImportError:
    print(
        "Error: 'click' is required for the CLI. Install it with: pip install click",
        file=sys.stderr,
    )
    sys.exit(1)


# Log level colors for terminal output
LEVEL_COLORS = {
    "DEBUG": "white",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bright_red",
}

LEVEL_ICONS = {
    "DEBUG": "🔍",
    "INFO": "✅",
    "WARNING": "⚠️ ",
    "ERROR": "❌",
    "CRITICAL": "🔥",
}


def _parse_since(since: str) -> datetime:
    """
    Parse a human-readable time duration into a datetime.

    Supports: 1h, 30m, 2d, 1w (hours, minutes, days, weeks).
    """
    unit = since[-1].lower()
    try:
        value = int(since[:-1])
    except ValueError:
        raise click.BadParameter(f"Invalid duration: {since}. Use format like '1h', '30m', '2d'")

    now = datetime.now(timezone.utc)
    if unit == "h":
        return now - timedelta(hours=value)
    elif unit == "m":
        return now - timedelta(minutes=value)
    elif unit == "d":
        return now - timedelta(days=value)
    elif unit == "w":
        return now - timedelta(weeks=value)
    else:
        raise click.BadParameter(f"Unknown time unit: {unit}. Use h/m/d/w")


def _api_request(endpoint: str, path: str, params: dict) -> dict:
    """Make a GET request to the LogFoundry API."""
    # Build query string
    query_parts = []
    for k, v in params.items():
        if v is not None:
            if isinstance(v, datetime):
                query_parts.append(f"{k}={v.isoformat()}")
            else:
                query_parts.append(f"{k}={urllib.request.quote(str(v))}")

    url = f"{endpoint}{path}"
    if query_parts:
        url += "?" + "&".join(query_parts)

    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        click.echo(f"API error: {e.code} {e.reason}", err=True)
        return {}
    except urllib.error.URLError as e:
        click.echo(f"Connection error: {e.reason}", err=True)
        return {}


def _format_log_entry(entry: dict) -> str:
    """Format a single log entry for terminal display."""
    level = entry.get("level", "UNKNOWN")
    icon = LEVEL_ICONS.get(level, "📋")
    timestamp = entry.get("timestamp", "")
    service = entry.get("service", "unknown")
    message = entry.get("message", "")
    trace_id = entry.get("trace_id", "")

    # Truncate timestamp for display
    if "T" in timestamp:
        timestamp = timestamp.split("T")[1][:12]

    line = f"{icon} [{timestamp}] {service} | {level:8s} | {message}"
    if trace_id:
        line += f" (trace: {trace_id})"

    return line


@click.group()
@click.option(
    "--endpoint",
    default="http://localhost:8000",
    envvar="LOGFOUNDRY_ENDPOINT",
    help="LogFoundry API endpoint",
)
@click.pass_context
def cli(ctx: click.Context, endpoint: str):
    """LogFoundry CLI — Query and tail logs from your terminal."""
    ctx.ensure_object(dict)
    ctx.obj["endpoint"] = endpoint


@cli.command()
@click.option("--service", "-s", help="Filter by service name")
@click.option("--level", "-l", help="Filter by log level (DEBUG/INFO/WARNING/ERROR/CRITICAL)")
@click.option("--search", "-q", help="Full-text search on message")
@click.option("--since", help="Show logs since duration (e.g., 1h, 30m, 2d)")
@click.option("--limit", default=50, help="Max results per poll (default: 50)")
@click.option("--follow/--no-follow", "-f", default=True, help="Follow mode (poll continuously)")
@click.pass_context
def tail(
    ctx: click.Context,
    service: Optional[str],
    level: Optional[str],
    search: Optional[str],
    since: Optional[str],
    limit: int,
    follow: bool,
):
    """Tail logs in real-time with optional filters."""
    endpoint = ctx.obj["endpoint"]

    # Parse since duration
    since_dt = _parse_since(since) if since else None

    # Track last seen timestamp to avoid duplicates
    last_seen_ts = since_dt.isoformat() if since_dt else None
    # Python 3.7+ dicts preserve insertion order, allowing safe slicing
    seen_ids = {}

    click.echo(click.style("🔭 LogFoundry — Tailing logs...", bold=True, fg="cyan"))
    click.echo(f"   Endpoint: {endpoint}")
    if service:
        click.echo(f"   Service:  {service}")
    if level:
        click.echo(f"   Level:    {level}")
    if search:
        click.echo(f"   Search:   {search}")
    click.echo(click.style("─" * 80, fg="bright_black"))

    try:
        while True:
            params = {
                "service": service,
                "level": level,
                "search": search,
                "since": since_dt,
                "limit": limit,
            }

            data = _api_request(endpoint, "/query", params)
            results = data.get("results", [])

            # Filter out already-seen entries
            new_entries = []
            for entry in results:
                entry_id = entry.get("id")
                if entry_id and entry_id not in seen_ids:
                    seen_ids[entry_id] = True
                    new_entries.append(entry)

            # Print new entries (oldest first)
            for entry in reversed(new_entries):
                line = _format_log_entry(entry)
                color = LEVEL_COLORS.get(entry.get("level", ""), "white")
                click.echo(click.style(line, fg=color))

                # Update since to only get newer entries
                entry_ts = entry.get("timestamp")
                if entry_ts:
                    since_dt = datetime.fromisoformat(entry_ts.replace("Z", "+00:00"))

            if not follow:
                break

            # Limit seen_ids size to prevent memory growth (safe ordered slice via dict)
            if len(seen_ids) > 10000:
                seen_ids = dict(list(seen_ids.items())[-5000:])

            time.sleep(2)

    except KeyboardInterrupt:
        click.echo(click.style("\n👋 Stopped tailing.", fg="cyan"))


@cli.command()
@click.option("--service", "-s", help="Filter metrics by service name")
@click.pass_context
def stats(ctx: click.Context, service: Optional[str]):
    """Display log ingestion metrics."""
    endpoint = ctx.obj["endpoint"]

    click.echo(click.style("📊 LogFoundry Metrics", bold=True, fg="cyan"))
    click.echo(click.style("─" * 50, fg="bright_black"))

    try:
        req = urllib.request.Request(f"{endpoint}/metrics", method="GET")
        with urllib.request.urlopen(req, timeout=10) as response:
            metrics_text = response.read().decode("utf-8")

        # Parse and display Prometheus-format metrics
        for line in metrics_text.strip().split("\n"):
            if line.startswith("#"):
                # Comment/type lines
                if line.startswith("# HELP"):
                    click.echo(click.style(f"\n{line.split('# HELP ')[1]}", fg="bright_black"))
                continue

            if service and f'service="{service}"' not in line and "total" not in line:
                continue

            # Format metric line
            if "{" in line:
                # Labeled metric
                name, rest = line.split("{", 1)
                labels, value = rest.rsplit("}", 1)
                click.echo(f"  {click.style(name, fg='green')}{{{labels}}} = {click.style(value.strip(), bold=True)}")
            else:
                # Simple metric
                parts = line.rsplit(" ", 1)
                if len(parts) == 2:
                    click.echo(f"  {click.style(parts[0], fg='green')} = {click.style(parts[1], bold=True)}")

    except urllib.error.URLError as e:
        click.echo(f"Connection error: {e.reason}", err=True)
    except KeyboardInterrupt:
        pass

    click.echo(click.style("─" * 50, fg="bright_black"))


def main():
    """Entry point for the logfoundry CLI."""
    cli()


if __name__ == "__main__":
    main()
