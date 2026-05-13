from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class EventFilters:
    product_codes: list[str] = field(default_factory=list)
    event_types: list[str] = field(default_factory=list)
    channels: list[str] = field(default_factory=list)


@dataclass(slots=True)
class OverviewStats:
    total_rows: int
    rows_1m: int
    rows_5m: int
    rows_15m: int
    latest_received_at: str | None


@dataclass(slots=True)
class GroupCount:
    key: str
    count: int


@dataclass(slots=True)
class FreshnessRow:
    event_type: str
    product_code: str
    latest_received_at: str | None
    age_seconds: int | None


@dataclass(slots=True)
class LatestEvent:
    received_at: str
    event_type: str
    product_code: str
    channel: str
    collector_instance_id: str
    payload_hash: str
    payload_json: str


@dataclass(slots=True)
class ThroughputRow:
    minute_bucket: str
    event_type: str
    product_code: str
    count: int


@dataclass(slots=True)
class DashboardData:
    overview: OverviewStats
    by_event_type: list[GroupCount]
    by_product_code: list[GroupCount]
    freshness: list[FreshnessRow]
    latest_events: list[LatestEvent]
    throughput: list[ThroughputRow]

