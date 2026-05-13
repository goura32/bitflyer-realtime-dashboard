from __future__ import annotations

import json

from textual import on
from textual.app import App, ComposeResult
from textual.containers import Horizontal
from textual.reactive import reactive
from textual.widgets import DataTable, Footer, Header, Pretty, Static

from bitflyer_realtime_dashboard.clickhouse_client import DashboardRepository
from bitflyer_realtime_dashboard.config import AppConfig
from bitflyer_realtime_dashboard.models import DashboardData, EventFilters
from bitflyer_realtime_dashboard.rendering import (
    filters_to_text,
    render_alert_panel,
    render_board_panel,
    render_collector_bias_panel,
    render_collector_panel,
    render_executions_panel,
    render_freshness,
    render_market_panel,
    render_overview,
    render_throughput,
)


class DashboardApp(App[None]):
    CSS = """
    Screen {
      layout: vertical;
    }
    #top {
      height: 16;
    }
    #middle {
        height: 13;
    }
    #lower {
      height: 14;
    }
    #deep {
      height: 18;
    }
    #bottom {
      height: 1fr;
    }
    .panel {
      border: round $accent;
      padding: 0 1;
    }
    #latest {
      height: 1fr;
    }
    #json_detail {
      min-width: 40;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("1", "lookback_1m", "1m"),
        ("5", "lookback_5m", "5m"),
        ("f", "lookback_15m", "15m"),
        ("0", "lookback_all", "All"),
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
            yield Static(classes="panel", id="market")
        with Horizontal(id="middle"):
            yield Static(classes="panel", id="alerts")
            yield Static(classes="panel", id="collectors")
            yield Static(classes="panel", id="throughput")
        with Horizontal(id="lower"):
            yield Static(classes="panel", id="collector_bias")
            yield Static(classes="panel", id="executions")
        with Horizontal(id="deep"):
            yield Static(classes="panel", id="freshness")
            yield Static(classes="panel", id="board")
        with Horizontal(id="bottom"):
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
        self._update_subtitle()
        self.call_after_refresh(self.refresh_data)

    def _update_subtitle(self) -> None:
        self.sub_title = filters_to_text(
            self.filters.product_codes,
            self.filters.event_types,
            self.filters.channels,
            self.filters.since_minutes,
        )

    def action_refresh(self) -> None:
        self.refresh_data()

    def action_lookback_1m(self) -> None:
        self.filters.since_minutes = 1
        self._update_subtitle()
        self.refresh_data()

    def action_lookback_5m(self) -> None:
        self.filters.since_minutes = 5
        self._update_subtitle()
        self.refresh_data()

    def action_lookback_15m(self) -> None:
        self.filters.since_minutes = 15
        self._update_subtitle()
        self.refresh_data()

    def action_lookback_all(self) -> None:
        self.filters.since_minutes = None
        self._update_subtitle()
        self.refresh_data()

    def refresh_data(self) -> None:
        data = self.repository.fetch_dashboard_data(self.filters, self.limit)
        self.latest_data = data
        series = self.repository.throughput_by_series(data.throughput)

        self.query_one("#overview", Static).update(
            render_overview(
                data,
                filters_text=filters_to_text(
                    self.filters.product_codes,
                    self.filters.event_types,
                    self.filters.channels,
                    self.filters.since_minutes,
                ),
            )
        )
        self.query_one("#market", Static).update(render_market_panel(data.ticker_points))
        self.query_one("#alerts", Static).update(render_alert_panel(data))
        self.query_one("#collectors", Static).update(
            render_collector_panel(data, self.config.dashboard.collector_stale_seconds)
        )
        self.query_one("#throughput", Static).update(render_throughput(data, series))
        self.query_one("#collector_bias", Static).update(render_collector_bias_panel(data))
        self.query_one("#executions", Static).update(render_executions_panel(data.executions))
        self.query_one("#freshness", Static).update(
            render_freshness(data, self.config.dashboard)
        )
        self.query_one("#board", Static).update(render_board_panel(data.board_snapshots))
        self.query_one("#latest", DataTable).clear()
        latest = self.query_one("#latest", DataTable)
        for event in data.latest_events:
            latest.add_row(
                event.received_at,
                event.event_type,
                event.product_code,
                event.channel,
                event.collector_instance_id,
            )
        latest.cursor_type = "row"
        latest.show_cursor = True
        latest.border_title = "Latest Events"
        latest.border_subtitle = "Enter highlight moves JSON detail"
        self.query_one("#json_detail", Pretty).border_title = "JSON Detail"
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
                "payload_json": json.loads(event.payload_json),
            }
        )

    def on_unmount(self) -> None:
        self.repository.close()
