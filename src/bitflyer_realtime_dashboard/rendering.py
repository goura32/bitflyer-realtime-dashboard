from __future__ import annotations

import json
from collections import defaultdict

from rich.columns import Columns
from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from bitflyer_realtime_dashboard.config import DashboardConfig
from bitflyer_realtime_dashboard.formatting import (
    age_style,
    format_price,
    format_size,
    sparkline,
    style_event_type,
)
from bitflyer_realtime_dashboard.models import (
    BoardDeltaView,
    BoardSnapshotView,
    CollectorBiasRow,
    DashboardData,
    ExecutionSummary,
    LatestEvent,
    TickerPoint,
)


def _format_change(current: float | None, previous: float | None) -> Text:
    if current is None or previous is None:
        return Text("-", style="dim")
    change = current - previous
    if change > 0:
        return Text(f"+{format_price(change)}", style="green")
    if change < 0:
        return Text(format_price(change), style="red")
    return Text(format_price(change), style="dim")


def _stale_threshold(config: DashboardConfig, event_type: str) -> int:
    thresholds = {
        "ticker": config.ticker_stale_seconds,
        "executions": config.executions_stale_seconds,
        "board_delta": config.board_delta_stale_seconds,
        "board_snapshot": config.board_snapshot_stale_seconds,
    }
    return thresholds.get(event_type, config.stale_after_seconds)


def render_overview(
    data: DashboardData,
    dedupe_view: str = "both",
    filters_text: str = "all",
) -> Panel:
    if dedupe_view == "unique":
        total = data.overview.unique_total_rows
        rows_window = (
            data.overview.unique_rows_1m,
            data.overview.unique_rows_5m,
            data.overview.unique_rows_15m,
        )
        mode_text = "unique payload_hash"
    elif dedupe_view == "raw":
        total = data.overview.total_rows
        rows_window = (
            data.overview.rows_1m,
            data.overview.rows_5m,
            data.overview.rows_15m,
        )
        mode_text = "raw rows"
    else:
        total = data.overview.total_rows
        rows_window = (
            data.overview.rows_1m,
            data.overview.rows_5m,
            data.overview.rows_15m,
        )
        mode_text = "both"
    table = Table.grid(expand=True)
    table.add_column(style="bold")
    table.add_column()
    table.add_row("Scope", filters_text)
    table.add_row("Mode", mode_text)
    table.add_row("Total", str(total))
    table.add_row(
        "1m / 5m / 15m",
        f"{rows_window[0]} / {rows_window[1]} / {rows_window[2]}",
    )
    table.add_row("Latest", str(data.overview.latest_received_at))
    table.add_row(
        "Dedupe",
        (
            f"all {data.dedupe.total_rows}/{data.dedupe.unique_hashes}"
            f" dup={data.dedupe.duplicate_rows}"
        ),
    )
    table.add_row(
        "Recent Dedupe",
        (
            f"window {data.dedupe.recent_rows}/{data.dedupe.recent_unique_hashes}"
            f" dup={data.dedupe.recent_duplicate_rows}"
        ),
    )
    table.add_row(
        "Event Types",
        "  ".join(f"{item.key}:{item.count}" for item in data.by_event_type) or "-",
    )
    table.add_row(
        "Products",
        "  ".join(f"{item.key}:{item.count}" for item in data.by_product_code) or "-",
    )
    return Panel(table, title="Overview", border_style="cyan")


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


def render_freshness(data: DashboardData, config: DashboardConfig) -> Table:
    event_types = sorted({row.event_type for row in data.freshness})
    product_codes = sorted({row.product_code for row in data.freshness})
    matrix = {(row.event_type, row.product_code): row for row in data.freshness}

    table = Table(title="Freshness Matrix", expand=True)
    table.add_column("event_type", style="bold")
    for product_code in product_codes:
        table.add_column(product_code, justify="center")

    for event_type in event_types:
        cells: list[Text] = []
        for product_code in product_codes:
            row = matrix.get((event_type, product_code))
            if row is None:
                cells.append(Text("-", style="dim"))
                continue
            style = age_style(row.age_seconds, _stale_threshold(config, event_type))
            cell = Text(f"{row.age_seconds}s", style=style)
            cells.append(cell)
        table.add_row(event_type, *cells)
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
        return Panel("No event selected", title="JSON Detail", border_style="bright_black")
    pretty = json.dumps(json.loads(event.payload_json), indent=2, ensure_ascii=False)
    return Panel(pretty, title=f"JSON Detail: {event.channel}", border_style="bright_black")


def render_throughput(
    data: DashboardData,
    series: dict[str, list[int]],
    dedupe_view: str = "both",
) -> Table:
    title = "Throughput"
    if dedupe_view == "unique":
        title = "Throughput (Unique)"
    elif dedupe_view == "raw":
        title = "Throughput (Raw)"
    table = Table(title=title)
    table.add_column("Series")
    table.add_column("Sparkline")
    table.add_column("Last", justify="right")
    for key, values in sorted(series.items()):
        table.add_row(key, sparkline(values), str(values[-1] if values else 0))
    return table


def render_market_panel(ticker_points: list[TickerPoint]) -> Panel:
    grouped: dict[str, list[TickerPoint]] = defaultdict(list)
    for point in ticker_points:
        grouped[point.product_code].append(point)

    if not grouped:
        return Panel("No ticker data", title="Market", border_style="green")

    table = Table(expand=True)
    table.add_column("Product")
    table.add_column("Last", justify="right")
    table.add_column("Bid")
    table.add_column("Ask")
    table.add_column("Spread", justify="right")
    table.add_column("Chart")

    for product_code, points in sorted(grouped.items()):
        latest = points[-1]
        spread = None
        if latest.best_bid is not None and latest.best_ask is not None:
            spread = latest.best_ask - latest.best_bid
        table.add_row(
            product_code,
            format_price(latest.ltp),
            format_price(latest.best_bid),
            format_price(latest.best_ask),
            format_price(spread),
            sparkline([point.ltp for point in points]),
        )
    return Panel(table, title="Market", border_style="green")


def render_alert_panel(data: DashboardData) -> Panel:
    table = Table(expand=True)
    table.add_column("Severity", width=10)
    table.add_column("Scope")
    table.add_column("Message")
    if not data.alerts:
        table.add_row("[green]ok[/green]", "-", "no active alerts")
    else:
        for alert in data.alerts[:8]:
            style = "red" if alert.severity == "critical" else "yellow"
            table.add_row(
                f"[{style}]{alert.severity}[/{style}]",
                alert.scope,
                alert.message,
            )
    return Panel(table, title="Alerts", border_style="red" if data.alerts else "green")


def render_collector_panel(data: DashboardData, collector_stale_seconds: int) -> Panel:
    table = Table(expand=True)
    table.add_column("Collector")
    table.add_column("Age", justify="right")
    table.add_column("1m", justify="right")
    table.add_column("15m", justify="right")
    table.add_column("Total", justify="right")
    if not data.collectors:
        table.add_row("-", "-", "-", "-", "-")
    else:
        for row in data.collectors:
            style = age_style(row.age_seconds, collector_stale_seconds)
            table.add_row(
                row.collector_instance_id,
                Text(str(row.age_seconds), style=style),
                str(row.rows_1m),
                str(row.rows_15m),
                str(row.total_rows),
            )
    return Panel(table, title="Collectors", border_style="blue")


def _render_bias_table(rows: list[CollectorBiasRow]) -> Table:
    table = Table(expand=True)
    table.add_column("Collector")
    table.add_column("Key")
    table.add_column("Count", justify="right")
    table.add_column("Share", justify="right")
    if not rows:
        table.add_row("-", "-", "-", "-")
    else:
        for row in rows:
            table.add_row(
                row.collector_instance_id,
                row.group_key,
                str(row.event_count),
                f"{row.share_ratio * 100:.1f}%",
            )
    return table


def render_collector_bias_panel(data: DashboardData) -> Panel:
    table = _render_bias_table(data.collector_product_bias[:12])
    return Panel(table, title="Collector Bias", border_style="blue")


def render_collector_event_type_bias_panel(data: DashboardData) -> Panel:
    table = _render_bias_table(data.collector_event_type_bias[:12])
    return Panel(table, title="Collector Event Bias", border_style="blue")


def render_executions_panel(executions: list[ExecutionSummary]) -> Panel:
    if not executions:
        return Panel("No executions data", title="Executions", border_style="magenta")
    table = Table(expand=True)
    table.add_column("Product")
    table.add_column("Last", justify="right")
    table.add_column("Range")
    table.add_column("Trades", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Chart")
    for row in executions:
        table.add_row(
            row.product_code,
            format_price(row.latest_price),
            f"{format_price(row.min_price)} - {format_price(row.max_price)}",
            str(row.trade_count),
            format_size(row.total_size),
            sparkline(row.price_series),
        )
    return Panel(table, title="Executions", border_style="magenta")


def render_board_panel(board_snapshots: list[BoardSnapshotView]) -> Panel:
    if not board_snapshots:
        return Panel("No board snapshots", title="Board", border_style="yellow")

    panels = []
    for board in board_snapshots:
        board_table = Table.grid(expand=True)
        board_table.add_column(justify="right", style="red")
        board_table.add_column(justify="right")
        board_table.add_column(justify="right", style="green")
        board_table.add_row("Ask Px", "Ask Sz", "")
        for ask in reversed(board.asks[:5]):
            board_table.add_row(format_price(ask.price), format_size(ask.size), "")
        board_table.add_row(
            Text("MID", style="bold yellow"),
            Text(format_price(board.mid_price), style="bold yellow"),
            Text(board.received_at.split(" ")[-1], style="dim"),
        )
        board_table.add_row("", "Bid Sz", "Bid Px")
        for bid in board.bids[:5]:
            board_table.add_row("", format_size(bid.size), format_price(bid.price))
        panels.append(Panel(board_table, title=board.product_code, border_style="yellow"))
    return Panel(
        Columns(panels, equal=True, expand=True),
        title="Board Snapshots",
        border_style="yellow",
    )


def render_board_delta_panel(board_deltas: list[BoardDeltaView]) -> Panel:
    if not board_deltas:
        return Panel("No board deltas", title="Board Deltas", border_style="magenta")

    latest_by_product: dict[str, BoardDeltaView] = {}
    for board in board_deltas:
        latest_by_product.setdefault(board.product_code, board)

    panels = []
    for product_code in sorted(latest_by_product):
        board = latest_by_product[product_code]
        delta_table = Table.grid(expand=True)
        delta_table.add_column(justify="right", style="red")
        delta_table.add_column(justify="right")
        delta_table.add_column(justify="right", style="green")
        delta_table.add_row(
            Text("MID", style="bold magenta"),
            Text(format_price(board.mid_price), style="bold magenta"),
            _format_change(board.mid_price, board.previous_mid_price),
        )
        delta_table.add_row(
            Text("Spread", style="bold"),
            Text(format_price(board.spread), style="bold"),
            _format_change(board.spread, board.previous_spread),
        )
        delta_table.add_row("Ask Chg", "Ask Sz", "")
        for ask in reversed(board.asks[:5]):
            delta_table.add_row(format_price(ask.price), format_size(ask.size), "")
        delta_table.add_row(
            Text("At", style="dim"),
            Text(board.received_at.split(" ")[-1], style="dim"),
            "",
        )
        delta_table.add_row("", "Bid Sz", "Bid Chg")
        for bid in board.bids[:5]:
            delta_table.add_row("", format_size(bid.size), format_price(bid.price))
        panels.append(Panel(delta_table, title=board.product_code, border_style="magenta"))
    return Panel(
        Columns(panels, equal=True, expand=True),
        title="Board Deltas",
        border_style="magenta",
    )


def filters_to_text(
    product_codes: list[str],
    event_types: list[str],
    channels: list[str],
    since_minutes: int | None,
) -> str:
    parts = []
    if product_codes:
        parts.append("products=" + ",".join(product_codes))
    if event_types:
        parts.append("types=" + ",".join(event_types))
    if channels:
        parts.append("channels=" + ",".join(channels))
    if since_minutes is not None:
        parts.append(f"since={since_minutes}m")
    return " | ".join(parts) if parts else "all"


def render_compact_watch(
    data: DashboardData,
    series: dict[str, list[int]],
    config: DashboardConfig,
    dedupe_view: str = "both",
    filters_text: str = "all",
) -> Group:
    return Group(
        render_overview(data, dedupe_view=dedupe_view, filters_text=filters_text),
        render_alert_panel(data),
        render_market_panel(data.ticker_points),
        render_collector_panel(data, config.collector_stale_seconds),
        render_collector_bias_panel(data),
        render_collector_event_type_bias_panel(data),
        render_executions_panel(data.executions),
        render_freshness(data, config),
        render_throughput(data, series, dedupe_view=dedupe_view),
        render_board_panel(data.board_snapshots),
        render_board_delta_panel(data.board_deltas),
        render_latest_events(data.latest_events),
    )
