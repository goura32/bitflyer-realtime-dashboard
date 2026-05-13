from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from dotenv import dotenv_values
from pydantic import BaseModel, ConfigDict, Field


class ClickHouseConfig(BaseModel):
    host: str = "127.0.0.1"
    port: int = 8123
    database: str = "clickhouse"
    username: str = "clickhouse"
    password: str = ""


class DashboardConfig(BaseModel):
    refresh_seconds: float = 2.0
    stale_after_seconds: int = 15
    ticker_stale_seconds: int = 15
    executions_stale_seconds: int = 30
    board_delta_stale_seconds: int = 30
    board_snapshot_stale_seconds: int = 150
    collector_stale_seconds: int = 30
    default_limit: int = 20
    chart_points: int = 30
    board_depth: int = 5
    dedupe_view: Literal["raw", "unique", "both"] = "both"
    product_codes: list[str] = Field(default_factory=list)
    event_types: list[str] = Field(default_factory=list)
    channels: list[str] = Field(default_factory=list)


class AppConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    clickhouse: ClickHouseConfig = Field(default_factory=ClickHouseConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)


def _load_yaml(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as fp:
        data = yaml.safe_load(fp) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping")
    return data


def _load_env(path: Path | None) -> dict[str, str]:
    if path is not None and path.exists():
        return {k: v for k, v in dotenv_values(path).items() if v is not None}
    default_env = Path(".env")
    if default_env.exists():
        return {k: v for k, v in dotenv_values(default_env).items() if v is not None}
    return {}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _apply_env(data: dict[str, Any], env: dict[str, str]) -> dict[str, Any]:
    clickhouse = data.get("clickhouse", {})
    dashboard = data.get("dashboard", {})
    mapping = {
        "CLICKHOUSE_HOST": ("clickhouse", "host", str),
        "CLICKHOUSE_PORT": ("clickhouse", "port", int),
        "CLICKHOUSE_DATABASE": ("clickhouse", "database", str),
        "CLICKHOUSE_USERNAME": ("clickhouse", "username", str),
        "CLICKHOUSE_PASSWORD": ("clickhouse", "password", str),
        "REFRESH_SECONDS": ("dashboard", "refresh_seconds", float),
        "STALE_AFTER_SECONDS": ("dashboard", "stale_after_seconds", int),
        "TICKER_STALE_SECONDS": ("dashboard", "ticker_stale_seconds", int),
        "EXECUTIONS_STALE_SECONDS": ("dashboard", "executions_stale_seconds", int),
        "BOARD_DELTA_STALE_SECONDS": ("dashboard", "board_delta_stale_seconds", int),
        "BOARD_SNAPSHOT_STALE_SECONDS": ("dashboard", "board_snapshot_stale_seconds", int),
        "COLLECTOR_STALE_SECONDS": ("dashboard", "collector_stale_seconds", int),
        "DEDUPE_VIEW": ("dashboard", "dedupe_view", str),
    }
    for key, (section, field_name, caster) in mapping.items():
        if key not in env:
            continue
        value = caster(env[key])
        if section == "clickhouse":
            clickhouse[field_name] = value
        else:
            dashboard[field_name] = value
    data["clickhouse"] = clickhouse
    data["dashboard"] = dashboard
    return data


def load_config(config_path: Path | None = None, env_path: Path | None = None) -> AppConfig:
    merged = _deep_merge({}, _load_yaml(config_path))
    merged = _apply_env(merged, _load_env(env_path))
    return AppConfig.model_validate(merged)
