from __future__ import annotations

import time
from pathlib import Path
from typing import Literal

import typer
from rich.console import Console

from bitflyer_realtime_dashboard.clickhouse_client import DashboardRepository
from bitflyer_realtime_dashboard.config import AppConfig, load_config
from bitflyer_realtime_dashboard.dashboard_app import DashboardApp
from bitflyer_realtime_dashboard.models import EventFilters
from bitflyer_realtime_dashboard.rendering import (
    filters_to_text,
    render_alert_panel,
    render_board_delta_panel,
    render_board_panel,
    render_collector_bias_panel,
    render_collector_event_type_bias_panel,
    render_collector_panel,
    render_compact_watch,
    render_executions_panel,
    render_freshness,
    render_group_counts,
    render_json_detail,
    render_latest_events,
    render_market_panel,
    render_overview,
    render_throughput,
)

app = typer.Typer(help="CLI dashboard for latest bitFlyer data stored in ClickHouse")
console = Console()


def _load_config(config_path: Path | None, env_path: Path | None) -> AppConfig:
    return load_config(config_path=config_path, env_path=env_path)


def _filters(
    config: AppConfig,
    product_code: list[str] | None,
    event_type: list[str] | None,
    channel: list[str] | None,
    since_minutes: int | None,
) -> EventFilters:
    return EventFilters(
        product_codes=product_code or config.dashboard.product_codes,
        event_types=event_type or config.dashboard.event_types,
        channels=channel or config.dashboard.channels,
        since_minutes=since_minutes,
    )


@app.command()
def doctor(
    config: Path | None = typer.Option(None, "--config"),
    env_file: Path | None = typer.Option(None, "--env-file"),
) -> None:
    cfg = _load_config(config, env_file)
    repo = DashboardRepository(cfg)
    try:
        result = repo.doctor()
    finally:
        repo.close()
    console.print(f"ClickHouse version: {result['version']}")
    console.print("Tables:")
    for table in result["tables"]:
        console.print(f"- {table}")


@app.command()
def snapshot(
    config: Path | None = typer.Option(None, "--config"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    limit: int | None = typer.Option(None, "--limit"),
    product_code: list[str] = typer.Option(None, "--product-code"),
    event_type: list[str] = typer.Option(None, "--event-type"),
    channel: list[str] = typer.Option(None, "--channel"),
    since_minutes: int | None = typer.Option(None, "--since-minutes"),
    dedupe_view: Literal["raw", "unique", "both"] | None = typer.Option(
        None,
        "--dedupe-view",
    ),
) -> None:
    cfg = _load_config(config, env_file)
    if dedupe_view is not None:
        cfg.dashboard.dedupe_view = dedupe_view
    filters = _filters(cfg, product_code, event_type, channel, since_minutes)
    with DashboardRepository(cfg) as repo:
        data = repo.fetch_dashboard_data(filters, limit or cfg.dashboard.default_limit)
        series = repo.throughput_by_series(
            data.throughput,
            dedupe_view=cfg.dashboard.dedupe_view,
        )
    console.print(
        render_overview(
            data,
            dedupe_view=cfg.dashboard.dedupe_view,
            filters_text=filters_to_text(
                filters.product_codes,
                filters.event_types,
                filters.channels,
                filters.since_minutes,
            ),
        )
    )
    console.print(render_alert_panel(data))
    console.print(render_market_panel(data.ticker_points))
    console.print(render_collector_panel(data, cfg.dashboard.collector_stale_seconds))
    console.print(render_collector_bias_panel(data))
    console.print(render_collector_event_type_bias_panel(data))
    console.print(render_executions_panel(data.executions))
    console.print(render_group_counts("By Event Type", data.by_event_type))
    console.print(render_group_counts("By Product Code", data.by_product_code))
    console.print(render_freshness(data, cfg.dashboard))
    console.print(render_throughput(data, series, dedupe_view=cfg.dashboard.dedupe_view))
    console.print(render_board_delta_panel(data.board_deltas))
    console.print(render_board_panel(data.board_snapshots))
    console.print(render_latest_events(data.latest_events))
    if data.latest_events:
        console.print(render_json_detail(data.latest_events[0]))


@app.command()
def latest(
    config: Path | None = typer.Option(None, "--config"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    limit: int | None = typer.Option(None, "--limit"),
    product_code: list[str] = typer.Option(None, "--product-code"),
    event_type: list[str] = typer.Option(None, "--event-type"),
    channel: list[str] = typer.Option(None, "--channel"),
    since_minutes: int | None = typer.Option(None, "--since-minutes"),
) -> None:
    cfg = _load_config(config, env_file)
    filters = _filters(cfg, product_code, event_type, channel, since_minutes)
    with DashboardRepository(cfg) as repo:
        events = repo.fetch_latest_events(filters, limit or cfg.dashboard.default_limit)
    console.print(render_latest_events(events))


@app.command()
def watch(
    config: Path | None = typer.Option(None, "--config"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    limit: int | None = typer.Option(None, "--limit"),
    refresh_seconds: float | None = typer.Option(None, "--refresh-seconds"),
    product_code: list[str] = typer.Option(None, "--product-code"),
    event_type: list[str] = typer.Option(None, "--event-type"),
    channel: list[str] = typer.Option(None, "--channel"),
    since_minutes: int | None = typer.Option(None, "--since-minutes"),
    dedupe_view: Literal["raw", "unique", "both"] | None = typer.Option(
        None,
        "--dedupe-view",
    ),
) -> None:
    cfg = _load_config(config, env_file)
    if refresh_seconds is not None:
        cfg.dashboard.refresh_seconds = refresh_seconds
    if dedupe_view is not None:
        cfg.dashboard.dedupe_view = dedupe_view
    filters = _filters(cfg, product_code, event_type, channel, since_minutes)
    with DashboardRepository(cfg) as repo, console.screen():
        while True:
            try:
                data = repo.fetch_dashboard_data(filters, limit or cfg.dashboard.default_limit)
                series = repo.throughput_by_series(
                    data.throughput,
                    dedupe_view=cfg.dashboard.dedupe_view,
                )
            except KeyboardInterrupt:
                raise
            console.clear()
            console.print(
                render_compact_watch(
                    data,
                    series,
                    cfg.dashboard,
                    dedupe_view=cfg.dashboard.dedupe_view,
                    filters_text=filters_to_text(
                        filters.product_codes,
                        filters.event_types,
                        filters.channels,
                        filters.since_minutes,
                    ),
                )
            )
            console.print("Press Ctrl+C to stop.")
            time.sleep(cfg.dashboard.refresh_seconds)


@app.command()
def tui(
    config: Path | None = typer.Option(None, "--config"),
    env_file: Path | None = typer.Option(None, "--env-file"),
    limit: int | None = typer.Option(None, "--limit"),
    refresh_seconds: float | None = typer.Option(None, "--refresh-seconds"),
    product_code: list[str] = typer.Option(None, "--product-code"),
    event_type: list[str] = typer.Option(None, "--event-type"),
    channel: list[str] = typer.Option(None, "--channel"),
    since_minutes: int | None = typer.Option(None, "--since-minutes"),
    dedupe_view: Literal["raw", "unique", "both"] | None = typer.Option(
        None,
        "--dedupe-view",
    ),
) -> None:
    cfg = _load_config(config, env_file)
    if refresh_seconds is not None:
        cfg.dashboard.refresh_seconds = refresh_seconds
    if dedupe_view is not None:
        cfg.dashboard.dedupe_view = dedupe_view
    filters = _filters(cfg, product_code, event_type, channel, since_minutes)
    DashboardApp(cfg, filters, limit or cfg.dashboard.default_limit).run()
