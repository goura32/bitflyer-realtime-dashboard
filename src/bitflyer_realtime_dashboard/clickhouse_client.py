from __future__ import annotations

from collections import defaultdict
from typing import Any

import clickhouse_connect

from bitflyer_realtime_dashboard.config import AppConfig
from bitflyer_realtime_dashboard.models import (
    DashboardData,
    EventFilters,
    FreshnessRow,
    GroupCount,
    LatestEvent,
    OverviewStats,
    ThroughputRow,
)


def build_where_clause(filters: EventFilters) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = []
    params: dict[str, Any] = {}
    if filters.product_codes:
        clauses.append("product_code IN %(product_codes)s")
        params["product_codes"] = filters.product_codes
    if filters.event_types:
        clauses.append("event_type IN %(event_types)s")
        params["event_types"] = filters.event_types
    if filters.channels:
        clauses.append("channel IN %(channels)s")
        params["channels"] = filters.channels
    if not clauses:
        return "", params
    return "WHERE " + " AND ".join(clauses), params


class DashboardRepository:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = clickhouse_connect.get_client(
            host=config.clickhouse.host,
            port=config.clickhouse.port,
            username=config.clickhouse.username,
            password=config.clickhouse.password,
            database=config.clickhouse.database,
        )

    def close(self) -> None:
        self.client.close()

    def doctor(self) -> dict[str, Any]:
        version = self.client.command("SELECT version()")
        tables = self.client.query(
            """
            SELECT name
            FROM system.tables
            WHERE database = %(database)s
              AND name IN (
                'raw_events',
                'raw_executions',
                'raw_tickers',
                'raw_board_snapshots',
                'raw_board_deltas'
              )
            ORDER BY name
            """,
            parameters={"database": self.config.clickhouse.database},
        ).result_rows
        return {"version": version, "tables": [row[0] for row in tables]}

    def fetch_dashboard_data(self, filters: EventFilters, limit: int) -> DashboardData:
        overview = self.fetch_overview(filters)
        by_event_type = self.fetch_group_counts(filters, "event_type")
        by_product_code = self.fetch_group_counts(filters, "product_code")
        freshness = self.fetch_freshness(filters)
        latest_events = self.fetch_latest_events(filters, limit)
        throughput = self.fetch_throughput(filters)
        return DashboardData(
            overview=overview,
            by_event_type=by_event_type,
            by_product_code=by_product_code,
            freshness=freshness,
            latest_events=latest_events,
            throughput=throughput,
        )

    def fetch_overview(self, filters: EventFilters) -> OverviewStats:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT
            count() AS total_rows,
            countIf(received_at >= now() - INTERVAL 1 MINUTE) AS rows_1m,
            countIf(received_at >= now() - INTERVAL 5 MINUTE) AS rows_5m,
            countIf(received_at >= now() - INTERVAL 15 MINUTE) AS rows_15m,
            max(received_at) AS latest_received_at
        FROM raw_events
        {where}
        """
        row = self.client.query(query, parameters=params).result_rows[0]
        latest = row[4].isoformat() if row[4] is not None else None
        return OverviewStats(
            total_rows=row[0],
            rows_1m=row[1],
            rows_5m=row[2],
            rows_15m=row[3],
            latest_received_at=latest,
        )

    def fetch_group_counts(self, filters: EventFilters, field_name: str) -> list[GroupCount]:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT {field_name}, count() AS count
        FROM raw_events
        {where}
        GROUP BY {field_name}
        ORDER BY count DESC, {field_name} ASC
        """
        rows = self.client.query(query, parameters=params).result_rows
        return [GroupCount(key=row[0], count=row[1]) for row in rows]

    def fetch_freshness(self, filters: EventFilters) -> list[FreshnessRow]:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT
            event_type,
            product_code,
            max(received_at) AS latest_received_at,
            dateDiff('second', max(received_at), now()) AS age_seconds
        FROM raw_events
        {where}
        GROUP BY event_type, product_code
        ORDER BY event_type, product_code
        """
        rows = self.client.query(query, parameters=params).result_rows
        results: list[FreshnessRow] = []
        for event_type, product_code, latest_received_at, age_seconds in rows:
            results.append(
                FreshnessRow(
                    event_type=event_type,
                    product_code=product_code,
                    latest_received_at=(
                        latest_received_at.isoformat() if latest_received_at else None
                    ),
                    age_seconds=age_seconds,
                )
            )
        return results

    def fetch_latest_events(self, filters: EventFilters, limit: int) -> list[LatestEvent]:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT
            toString(received_at) AS received_at,
            event_type,
            product_code,
            channel,
            collector_instance_id,
            payload_hash,
            payload_json
        FROM raw_events
        {where}
        ORDER BY received_at DESC
        LIMIT %(limit)s
        """
        params = {**params, "limit": limit}
        rows = self.client.query(query, parameters=params).result_rows
        return [
            LatestEvent(
                received_at=row[0],
                event_type=row[1],
                product_code=row[2],
                channel=row[3],
                collector_instance_id=row[4],
                payload_hash=row[5],
                payload_json=row[6],
            )
            for row in rows
        ]

    def fetch_throughput(self, filters: EventFilters) -> list[ThroughputRow]:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT
            formatDateTime(toStartOfMinute(received_at), '%Y-%m-%d %H:%M') AS minute_bucket,
            event_type,
            product_code,
            count() AS count
        FROM raw_events
        {where}
          {"AND" if where else "WHERE"} received_at >= now() - INTERVAL 15 MINUTE
        GROUP BY minute_bucket, event_type, product_code
        ORDER BY minute_bucket ASC, event_type ASC, product_code ASC
        """
        rows = self.client.query(query, parameters=params).result_rows
        return [
            ThroughputRow(
                minute_bucket=row[0],
                event_type=row[1],
                product_code=row[2],
                count=row[3],
            )
            for row in rows
        ]

    def throughput_by_series(self, rows: list[ThroughputRow]) -> dict[str, list[int]]:
        minute_buckets = sorted({row.minute_bucket for row in rows})
        indexed = {bucket: i for i, bucket in enumerate(minute_buckets)}
        series: dict[str, list[int]] = defaultdict(lambda: [0] * len(minute_buckets))
        for row in rows:
            key = f"{row.event_type}:{row.product_code}"
            series[key][indexed[row.minute_bucket]] = row.count
        return dict(series)
