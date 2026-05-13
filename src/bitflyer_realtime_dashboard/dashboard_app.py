from __future__ import annotations

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Pretty, Static

from bitflyer_realtime_dashboard.clickhouse_client import DashboardRepository
from bitflyer_realtime_dashboard.config import AppConfig
from bitflyer_realtime_dashboard.models import DashboardData, EventFilters


class DashboardApp(App[None]):
    CSS = """
    Screen {
      layout: vertical;
    }
    #top {
      height: 14;
    }
    #bottom {
      height: 1fr;
    }
    .panel {
      border: round $accent;
      padding: 1;
    }
    DataTable {
      height: 1fr;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
    ]

    selected_event_index = reactive(0)

    def __init__(self, config: AppConfig, filters: EventFilters, limit: int) -> None:
        super().__init__()
        self.config = config
        self.filters = filters
        self.limit = limit
        self.repository = DashboardRepository(config)
        self.latest_data: DashboardData | None = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Horizontal(id="top"):
            yield Static(classes="panel", id="overview")
            yield Static(classes="panel", id="throughput")
        with Horizontal(id="bottom"):
            with Vertical():
                yield Static(classes="panel", id="freshness")
                latest = DataTable(id="latest")
                latest.zebra_stripes = True
                yield latest
            yield Pretty({}, id="json_detail")
        yield Footer()

    def on_mount(self) -> None:
        latest = self.query_one("#latest", DataTable)
        latest.add_columns(
            "received_at",
            "event_type",
            "product_code",
            "channel",
            "collector",
        )
        self.set_interval(self.config.dashboard.refresh_seconds, self.refresh_data)
        self.call_after_refresh(self.refresh_data)

    def action_refresh(self) -> None:
        self.refresh_data()

    def refresh_data(self) -> None:
        data = self.repository.fetch_dashboard_data(self.filters, self.limit)
        self.latest_data = data

        overview = self.query_one("#overview", Static)
        overview.update(
            "\n".join(
                [
                    f"total rows: {data.overview.total_rows}",
                    f"rows 1m: {data.overview.rows_1m}",
                    f"rows 5m: {data.overview.rows_5m}",
                    f"rows 15m: {data.overview.rows_15m}",
                    f"latest: {data.overview.latest_received_at}",
                    "",
                    "event types:",
                    *[f"  {item.key}: {item.count}" for item in data.by_event_type],
                    "",
                    "product codes:",
                    *[f"  {item.key}: {item.count}" for item in data.by_product_code],
                ]
            )
        )

        freshness = self.query_one("#freshness", Static)
        freshness.update(
            "\n".join(
                [
                    "Freshness",
                    *[
                        (
                            f"{row.event_type:<14} {row.product_code:<12} "
                            f"{row.age_seconds:>4}s {row.latest_received_at}"
                        )
                        for row in data.freshness
                    ],
                ]
            )
        )

        throughput = self.query_one("#throughput", Static)
        series = self.repository.throughput_by_series(data.throughput)
        throughput.update(
            "\n".join(
                [
                    "Throughput 15m",
                    *[
                        f"{key:<32} {' '.join(str(v) for v in values[-5:])}"
                        for key, values in sorted(series.items())
                    ],
                ]
            )
        )

        latest = self.query_one("#latest", DataTable)
        latest.clear()
        for event in data.latest_events:
            latest.add_row(
                event.received_at,
                event.event_type,
                event.product_code,
                event.channel,
                event.collector_instance_id,
            )
        self.update_json_detail()

    @on(DataTable.RowHighlighted)
    def handle_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        self.selected_event_index = event.cursor_row
        self.update_json_detail()

    def update_json_detail(self) -> None:
        pretty = self.query_one("#json_detail", Pretty)
        if self.latest_data is None or not self.latest_data.latest_events:
            pretty.update({})
            return
        index = min(self.selected_event_index, len(self.latest_data.latest_events) - 1)
        event = self.latest_data.latest_events[index]
        pretty.update(
            {
                "received_at": event.received_at,
                "event_type": event.event_type,
                "product_code": event.product_code,
                "channel": event.channel,
                "collector_instance_id": event.collector_instance_id,
                "payload_hash": event.payload_hash,
                "payload_json": event.payload_json,
            }
        )

    def on_unmount(self) -> None:
        self.repository.close()
