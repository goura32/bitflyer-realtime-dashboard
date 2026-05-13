from __future__ import annotations

from pathlib import Path

import typer
from rich.console import Console

from bitflyer_realtime_dashboard.clickhouse_client import DashboardRepository
from bitflyer_realtime_dashboard.config import AppConfig, load_config
from bitflyer_realtime_dashboard.dashboard_app import DashboardApp
from bitflyer_realtime_dashboard.models import EventFilters
from bitflyer_realtime_dashboard.rendering import (
    render_freshness,
    render_group_counts,
    render_json_detail,
    render_latest_events,
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
) -> EventFilters:
    return EventFilters(
        product_codes=product_code or config.dashboard.product_codes,
        event_types=event_type or config.dashboard.event_types,
        channels=channel or config.dashboard.channels,
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
) -> None:
    cfg = _load_config(config, env_file)
    filters = _filters(cfg, product_code, event_type, channel)
    repo = DashboardRepository(cfg)
    try:
        data = repo.fetch_dashboard_data(filters, limit or cfg.dashboard.default_limit)
        series = repo.throughput_by_series(data.throughput)
    finally:
        repo.close()
    console.print(render_overview(data))
    console.print(render_group_counts("By Event Type", data.by_event_type))
    console.print(render_group_counts("By Product Code", data.by_product_code))
    console.print(render_freshness(data, cfg.dashboard.stale_after_seconds))
    console.print(render_throughput(data, series))
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
) -> None:
    cfg = _load_config(config, env_file)
    filters = _filters(cfg, product_code, event_type, channel)
    repo = DashboardRepository(cfg)
    try:
        events = repo.fetch_latest_events(filters, limit or cfg.dashboard.default_limit)
    finally:
        repo.close()
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
) -> None:
    cfg = _load_config(config, env_file)
    if refresh_seconds is not None:
        cfg.dashboard.refresh_seconds = refresh_seconds
    filters = _filters(cfg, product_code, event_type, channel)
    with console.screen():
        while True:
            repo = DashboardRepository(cfg)
            try:
                data = repo.fetch_dashboard_data(filters, limit or cfg.dashboard.default_limit)
                series = repo.throughput_by_series(data.throughput)
            finally:
                repo.close()
            console.clear()
            console.print(render_overview(data))
            console.print(render_freshness(data, cfg.dashboard.stale_after_seconds))
            console.print(render_throughput(data, series))
            console.print(render_latest_events(data.latest_events))
            console.print("Press Ctrl+C to stop.")
            import time

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
) -> None:
    cfg = _load_config(config, env_file)
    if refresh_seconds is not None:
        cfg.dashboard.refresh_seconds = refresh_seconds
    filters = _filters(cfg, product_code, event_type, channel)
    DashboardApp(cfg, filters, limit or cfg.dashboard.default_limit).run()

