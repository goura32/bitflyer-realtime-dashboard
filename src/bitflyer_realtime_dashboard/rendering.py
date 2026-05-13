from __future__ import annotations

import json

from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from bitflyer_realtime_dashboard.formatting import age_style, sparkline, style_event_type
from bitflyer_realtime_dashboard.models import DashboardData, LatestEvent


def render_overview(data: DashboardData) -> Table:
    table = Table(title="Overview")
    table.add_column("Metric")
    table.add_column("Value")
    table.add_row("total rows", str(data.overview.total_rows))
    table.add_row("rows 1m", str(data.overview.rows_1m))
    table.add_row("rows 5m", str(data.overview.rows_5m))
    table.add_row("rows 15m", str(data.overview.rows_15m))
    table.add_row("latest received_at", str(data.overview.latest_received_at))
    return table


def render_group_counts(title: str, counts: list) -> Table:
    table = Table(title=title)
    table.add_column("Key")
    table.add_column("Count", justify="right")
    for item in counts:
        if hasattr(item, "key") and hasattr(item, "count"):
            key = item.key
            count = item.count
        else:
            key = item[0]
            count = item[1]
        table.add_row(str(key), str(count))
    return table


def render_freshness(data: DashboardData, stale_after_seconds: int) -> Table:
    table = Table(title="Freshness")
    table.add_column("event_type")
    table.add_column("product_code")
    table.add_column("latest_received_at")
    table.add_column("age_seconds", justify="right")
    for row in data.freshness:
        style = age_style(row.age_seconds, stale_after_seconds)
        table.add_row(
            row.event_type,
            row.product_code,
            str(row.latest_received_at),
            Text(str(row.age_seconds), style=style),
        )
    return table


def render_latest_events(events: list[LatestEvent]) -> Table:
    table = Table(title="Latest Events")
    table.add_column("received_at")
    table.add_column("event_type")
    table.add_column("product_code")
    table.add_column("channel")
    table.add_column("collector")
    for event in events:
        table.add_row(
            event.received_at,
            style_event_type(event.event_type),
            event.product_code,
            event.channel,
            event.collector_instance_id,
        )
    return table


def render_json_detail(event: LatestEvent | None) -> Panel:
    if event is None:
        return Panel("No event selected", title="JSON Detail")
    pretty = json.dumps(json.loads(event.payload_json), indent=2, ensure_ascii=False)
    return Panel(pretty, title=f"JSON Detail: {event.channel}")


def render_throughput(data: DashboardData, series: dict[str, list[int]]) -> Table:
    table = Table(title="Throughput 15m")
    table.add_column("Series")
    table.add_column("Sparkline")
    table.add_column("Last Count", justify="right")
    for key, values in sorted(series.items()):
        table.add_row(key, sparkline(values), str(values[-1] if values else 0))
    return table
