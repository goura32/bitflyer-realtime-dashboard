from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

import clickhouse_connect

from bitflyer_realtime_dashboard.config import AppConfig
from bitflyer_realtime_dashboard.models import (
    AlertItem,
    BoardDeltaView,
    BoardLevel,
    BoardSnapshotView,
    CollectorBiasRow,
    CollectorStatus,
    DashboardData,
    DedupeStats,
    EventFilters,
    ExecutionSummary,
    FreshnessRow,
    GroupCount,
    LatestEvent,
    OverviewStats,
    ThroughputRow,
    TickerPoint,
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
    if filters.since_minutes is not None:
        clauses.append("received_at >= now() - toIntervalMinute(%(since_minutes)s)")
        params["since_minutes"] = filters.since_minutes
    if not clauses:
        return "", params
    return "WHERE " + " AND ".join(clauses), params


def parse_board_snapshot(
    payload_json: str,
    product_code: str,
    received_at: str,
    depth: int = 5,
) -> BoardSnapshotView:
    payload = json.loads(payload_json)
    bids = [
        BoardLevel(price=float(level["price"]), size=float(level["size"]))
        for level in payload.get("bids", [])[:depth]
    ]
    asks = [
        BoardLevel(price=float(level["price"]), size=float(level["size"]))
        for level in payload.get("asks", [])[:depth]
    ]
    mid_price = payload.get("mid_price")
    return BoardSnapshotView(
        product_code=product_code,
        received_at=received_at,
        mid_price=float(mid_price) if mid_price is not None else None,
        bids=bids,
        asks=asks,
    )


def parse_board_delta(
    payload_json: str,
    product_code: str,
    received_at: str,
    depth: int = 5,
) -> BoardDeltaView:
    payload = json.loads(payload_json)
    bids = [
        BoardLevel(price=float(level["price"]), size=float(level["size"]))
        for level in payload.get("bids", [])[:depth]
    ]
    asks = [
        BoardLevel(price=float(level["price"]), size=float(level["size"]))
        for level in payload.get("asks", [])[:depth]
    ]
    mid_price = payload.get("mid_price")
    best_bid = bids[0].price if bids else None
    best_ask = asks[0].price if asks else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    return BoardDeltaView(
        product_code=product_code,
        received_at=received_at,
        mid_price=float(mid_price) if mid_price is not None else None,
        best_bid=best_bid,
        best_ask=best_ask,
        spread=spread,
        bids=bids,
        asks=asks,
    )


def enrich_board_delta_views(deltas: list[BoardDeltaView]) -> list[BoardDeltaView]:
    enriched: list[BoardDeltaView] = []
    for current, previous in zip(deltas, deltas[1:], strict=False):
        enriched.append(
            BoardDeltaView(
                product_code=current.product_code,
                received_at=current.received_at,
                mid_price=current.mid_price,
                best_bid=current.best_bid,
                best_ask=current.best_ask,
                spread=current.spread,
                previous_mid_price=previous.mid_price,
                previous_spread=previous.spread,
                bids=current.bids,
                asks=current.asks,
            )
        )
    if deltas:
        last = deltas[-1]
        enriched.append(
            BoardDeltaView(
                product_code=last.product_code,
                received_at=last.received_at,
                mid_price=last.mid_price,
                best_bid=last.best_bid,
                best_ask=last.best_ask,
                spread=last.spread,
                bids=last.bids,
                asks=last.asks,
            )
        )
    return enriched


def summarize_execution_payload_rows(
    product_code: str,
    payload_rows: list[tuple[str, str]],
    points_per_product: int,
) -> ExecutionSummary:
    prices: list[float] = []
    total_size = 0.0
    trade_count = 0
    buy_count = 0
    sell_count = 0
    buy_size = 0.0
    sell_size = 0.0

    for _, payload_json in payload_rows:
        payload = json.loads(payload_json)
        executions = payload if isinstance(payload, list) else [payload]
        for execution in executions:
            if not isinstance(execution, dict):
                continue
            price = execution.get("price")
            size = execution.get("size")
            if price is None:
                continue
            side = str(execution.get("side", "")).upper()
            size_value = float(size) if size is not None else 0.0
            prices.append(float(price))
            total_size += size_value
            trade_count += 1
            if side == "BUY":
                buy_count += 1
                buy_size += size_value
            elif side == "SELL":
                sell_count += 1
                sell_size += size_value

    return ExecutionSummary(
        product_code=product_code,
        min_price=min(prices) if prices else None,
        max_price=max(prices) if prices else None,
        latest_price=prices[-1] if prices else None,
        total_size=total_size,
        trade_count=trade_count,
        buy_count=buy_count,
        sell_count=sell_count,
        buy_size=buy_size,
        sell_size=sell_size,
        price_series=prices[-points_per_product:],
    )


def stale_threshold_for_event_type(config: AppConfig, event_type: str) -> int:
    thresholds = {
        "ticker": config.dashboard.ticker_stale_seconds,
        "executions": config.dashboard.executions_stale_seconds,
        "board_delta": config.dashboard.board_delta_stale_seconds,
        "board_snapshot": config.dashboard.board_snapshot_stale_seconds,
    }
    return thresholds.get(event_type, config.dashboard.stale_after_seconds)


def build_alerts(
    config: AppConfig,
    freshness: list[FreshnessRow],
    collectors: list[CollectorStatus],
) -> list[AlertItem]:
    alerts: list[AlertItem] = []
    for row in freshness:
        threshold = stale_threshold_for_event_type(config, row.event_type)
        if row.age_seconds is not None and row.age_seconds >= threshold:
            severity = "critical" if row.age_seconds >= threshold * 2 else "warning"
            alerts.append(
                AlertItem(
                    scope=f"{row.event_type}:{row.product_code}",
                    severity=severity,
                    message=f"stale for {row.age_seconds}s (threshold {threshold}s)",
                    age_seconds=row.age_seconds,
                )
            )
    for collector in collectors:
        collector_threshold = config.dashboard.collector_stale_seconds
        if collector.age_seconds is not None and collector.age_seconds >= collector_threshold:
            severity = (
                "critical"
                if collector.age_seconds >= collector_threshold * 2
                else "warning"
            )
            alerts.append(
                AlertItem(
                    scope=collector.collector_instance_id,
                    severity=severity,
                    message=f"collector silent for {collector.age_seconds}s",
                    age_seconds=collector.age_seconds,
                )
            )
    return sorted(
        alerts,
        key=lambda item: (
            0 if item.severity == "critical" else 1,
            -(item.age_seconds or 0),
            item.scope,
        ),
    )


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

    def __enter__(self) -> DashboardRepository:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

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
        ticker_points = self.fetch_ticker_points(filters, self.config.dashboard.chart_points)
        board_snapshots = self.fetch_latest_board_snapshots(
            filters,
            depth=self.config.dashboard.board_depth,
        )
        board_deltas = self.fetch_latest_board_deltas(
            filters,
            depth=self.config.dashboard.board_depth,
        )
        executions = self.fetch_execution_summaries(filters, self.config.dashboard.chart_points)
        dedupe = self.fetch_dedupe_stats(filters)
        collectors = self.fetch_collector_status(filters)
        collector_product_bias = self.fetch_collector_bias(filters, "product_code")
        collector_event_type_bias = self.fetch_collector_bias(filters, "event_type")
        alerts = build_alerts(self.config, freshness, collectors)
        return DashboardData(
            overview=overview,
            by_event_type=by_event_type,
            by_product_code=by_product_code,
            freshness=freshness,
            latest_events=latest_events,
            throughput=throughput,
            ticker_points=ticker_points,
            board_snapshots=board_snapshots,
            board_deltas=board_deltas,
            executions=executions,
            dedupe=dedupe,
            collectors=collectors,
            collector_product_bias=collector_product_bias,
            collector_event_type_bias=collector_event_type_bias,
            alerts=alerts,
        )

    def fetch_overview(self, filters: EventFilters) -> OverviewStats:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT
            count() AS total_rows,
            countIf(received_at >= now() - INTERVAL 1 MINUTE) AS rows_1m,
            countIf(received_at >= now() - INTERVAL 5 MINUTE) AS rows_5m,
            countIf(received_at >= now() - INTERVAL 15 MINUTE) AS rows_15m,
            uniqExact(payload_hash) AS unique_total_rows,
            uniqExactIf(payload_hash, received_at >= now() - INTERVAL 1 MINUTE) AS unique_rows_1m,
            uniqExactIf(payload_hash, received_at >= now() - INTERVAL 5 MINUTE) AS unique_rows_5m,
            uniqExactIf(payload_hash, received_at >= now() - INTERVAL 15 MINUTE) AS unique_rows_15m,
            max(received_at) AS latest_received_at
        FROM raw_events
        {where}
        """
        row = self.client.query(query, parameters=params).result_rows[0]
        latest = row[8].isoformat() if row[8] is not None else None
        return OverviewStats(
            total_rows=row[0],
            rows_1m=row[1],
            rows_5m=row[2],
            rows_15m=row[3],
            unique_total_rows=row[4],
            unique_rows_1m=row[5],
            unique_rows_5m=row[6],
            unique_rows_15m=row[7],
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
            toString(received_at) AS received_at_text,
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
        window_minutes = filters.since_minutes or 15
        time_clause = (
            f'{"AND" if where else "WHERE"} '
            "received_at >= now() - toIntervalMinute(%(throughput_window)s)"
        )
        query = f"""
        SELECT
            formatDateTime(toStartOfMinute(received_at), '%%Y-%%m-%%d %%H:%%M') AS minute_bucket,
            event_type,
            product_code,
            count() AS count,
            uniqExact(payload_hash) AS unique_count
        FROM raw_events
        {where}
          {time_clause}
        GROUP BY minute_bucket, event_type, product_code
        ORDER BY minute_bucket ASC, event_type ASC, product_code ASC
        """
        params = {**params, "throughput_window": window_minutes}
        rows = self.client.query(query, parameters=params).result_rows
        return [
            ThroughputRow(
                minute_bucket=row[0],
                event_type=row[1],
                product_code=row[2],
                count=row[3],
                unique_count=row[4],
            )
            for row in rows
        ]

    def throughput_by_series(
        self,
        rows: list[ThroughputRow],
        dedupe_view: str = "both",
    ) -> dict[str, list[int]]:
        minute_buckets = sorted({row.minute_bucket for row in rows})
        indexed = {bucket: i for i, bucket in enumerate(minute_buckets)}
        series: dict[str, list[int]] = defaultdict(lambda: [0] * len(minute_buckets))
        for row in rows:
            key = f"{row.event_type}:{row.product_code}"
            value = row.unique_count if dedupe_view == "unique" else row.count
            series[key][indexed[row.minute_bucket]] = value
        return dict(series)

    def fetch_dedupe_stats(self, filters: EventFilters) -> DedupeStats:
        where, params = build_where_clause(filters)
        recent_minutes = filters.since_minutes or 15
        query = f"""
        SELECT
            count() AS total_rows,
            uniqExact(payload_hash) AS unique_hashes,
            count() - uniqExact(payload_hash) AS duplicate_rows,
            countIf(received_at >= now() - toIntervalMinute(%(recent_minutes)s)) AS recent_rows,
            uniqExactIf(payload_hash, received_at >= now() - toIntervalMinute(%(recent_minutes)s))
                AS recent_unique_hashes
        FROM raw_events
        {where}
        """
        params = {**params, "recent_minutes": recent_minutes}
        row = self.client.query(query, parameters=params).result_rows[0]
        recent_duplicates = row[3] - row[4]
        return DedupeStats(
            total_rows=row[0],
            unique_hashes=row[1],
            duplicate_rows=row[2],
            recent_rows=row[3],
            recent_unique_hashes=row[4],
            recent_duplicate_rows=recent_duplicates,
        )

    def fetch_collector_status(self, filters: EventFilters) -> list[CollectorStatus]:
        where, params = build_where_clause(filters)
        query = f"""
        SELECT
            collector_instance_id,
            count() AS total_rows,
            countIf(received_at >= now() - INTERVAL 1 MINUTE) AS rows_1m,
            countIf(received_at >= now() - INTERVAL 15 MINUTE) AS rows_15m,
            max(received_at) AS latest_received_at,
            dateDiff('second', max(received_at), now()) AS age_seconds
        FROM raw_events
        {where}
        GROUP BY collector_instance_id
        ORDER BY age_seconds ASC, collector_instance_id ASC
        """
        rows = self.client.query(query, parameters=params).result_rows
        return [
            CollectorStatus(
                collector_instance_id=row[0],
                total_rows=row[1],
                rows_1m=row[2],
                rows_15m=row[3],
                latest_received_at=row[4].isoformat() if row[4] else None,
                age_seconds=row[5],
            )
            for row in rows
        ]

    def fetch_ticker_points(
        self,
        filters: EventFilters,
        points_per_product: int = 30,
    ) -> list[TickerPoint]:
        if filters.event_types and "ticker" not in filters.event_types:
            return []
        product_filter = filters.product_codes
        params: dict[str, Any] = {"points_per_product": points_per_product}
        product_where = ""
        if product_filter:
            product_where = "AND product_code IN %(product_codes)s"
            params["product_codes"] = product_filter

        window_minutes = filters.since_minutes or 60
        params["ticker_window"] = window_minutes
        query = f"""
        SELECT
            product_code,
            toString(received_at) AS received_at,
            JSONExtractFloat(payload_json, 'ltp') AS ltp,
            JSONExtractFloat(payload_json, 'best_bid') AS best_bid,
            JSONExtractFloat(payload_json, 'best_ask') AS best_ask
        FROM (
            SELECT
                product_code,
                received_at,
                payload_json,
                row_number() OVER (PARTITION BY product_code ORDER BY received_at DESC) AS rn
            FROM raw_tickers
            WHERE received_at >= now() - toIntervalMinute(%(ticker_window)s)
              {product_where}
        )
        WHERE rn <= %(points_per_product)s
        ORDER BY product_code ASC, received_at ASC
        """
        rows = self.client.query(query, parameters=params).result_rows
        return [
            TickerPoint(
                product_code=row[0],
                received_at=row[1],
                ltp=float(row[2]),
                best_bid=float(row[3]) if row[3] is not None else None,
                best_ask=float(row[4]) if row[4] is not None else None,
            )
            for row in rows
        ]

    def fetch_latest_board_snapshots(
        self,
        filters: EventFilters,
        depth: int = 5,
        per_product_limit: int = 1,
    ) -> list[BoardSnapshotView]:
        if filters.event_types and "board_snapshot" not in filters.event_types:
            return []
        params: dict[str, Any] = {"per_product_limit": per_product_limit}
        product_where = ""
        if filters.product_codes:
            product_where = "AND product_code IN %(product_codes)s"
            params["product_codes"] = filters.product_codes
        query = f"""
        SELECT
            product_code,
            toString(received_at) AS received_at,
            payload_json
        FROM (
            SELECT
                product_code,
                received_at,
                payload_json,
                row_number() OVER (PARTITION BY product_code ORDER BY received_at DESC) AS rn
            FROM raw_board_snapshots
            WHERE 1 = 1
              {product_where}
        )
        WHERE rn <= %(per_product_limit)s
        ORDER BY product_code ASC, received_at DESC
        """
        rows = self.client.query(query, parameters=params).result_rows
        return [parse_board_snapshot(row[2], row[0], row[1], depth=depth) for row in rows]

    def fetch_latest_board_deltas(
        self,
        filters: EventFilters,
        depth: int = 5,
        per_product_limit: int = 2,
    ) -> list[BoardDeltaView]:
        if filters.event_types and "board_delta" not in filters.event_types:
            return []
        params: dict[str, Any] = {"per_product_limit": per_product_limit}
        product_where = ""
        if filters.product_codes:
            product_where = "AND product_code IN %(product_codes)s"
            params["product_codes"] = filters.product_codes
        query = f"""
        SELECT
            product_code,
            toString(received_at) AS received_at,
            payload_json
        FROM (
            SELECT
                product_code,
                received_at,
                payload_json,
                row_number() OVER (PARTITION BY product_code ORDER BY received_at DESC) AS rn
            FROM raw_board_deltas
            WHERE 1 = 1
              {product_where}
        )
        WHERE rn <= %(per_product_limit)s
        ORDER BY product_code ASC, received_at DESC
        """
        rows = self.client.query(query, parameters=params).result_rows
        grouped: dict[str, list[BoardDeltaView]] = defaultdict(list)
        for product_code, received_at, payload_json in rows:
            grouped[product_code].append(
                parse_board_delta(payload_json, product_code, received_at, depth=depth)
            )
        views: list[BoardDeltaView] = []
        for product_code in sorted(grouped):
            views.extend(enrich_board_delta_views(grouped[product_code]))
        return views

    def fetch_execution_summaries(
        self,
        filters: EventFilters,
        points_per_product: int = 30,
    ) -> list[ExecutionSummary]:
        if filters.event_types and "executions" not in filters.event_types:
            return []
        params: dict[str, Any] = {"points_per_product": points_per_product}
        product_where = ""
        if filters.product_codes:
            product_where = "AND product_code IN %(product_codes)s"
            params["product_codes"] = filters.product_codes
        window_minutes = filters.since_minutes or 60
        params["executions_window"] = window_minutes
        query = f"""
        SELECT
            product_code,
            toString(received_at) AS received_at,
            payload_json
        FROM (
            SELECT
                product_code,
                received_at,
                payload_json
            FROM (
                SELECT
                    product_code,
                    received_at,
                    payload_json,
                    row_number() OVER (PARTITION BY product_code ORDER BY received_at DESC) AS rn
                FROM raw_executions
                WHERE received_at >= now() - toIntervalMinute(%(executions_window)s)
                  {product_where}
            )
            WHERE rn <= %(points_per_product)s
            ORDER BY product_code ASC, received_at ASC
        )
        """
        rows = self.client.query(query, parameters=params).result_rows
        grouped: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for product_code, received_at, payload_json in rows:
            grouped[product_code].append((received_at, payload_json))

        return [
            summarize_execution_payload_rows(product_code, payload_rows, points_per_product)
            for product_code, payload_rows in sorted(grouped.items())
        ]

    def fetch_collector_bias(
        self,
        filters: EventFilters,
        dimension: str,
    ) -> list[CollectorBiasRow]:
        where, params = build_where_clause(filters)
        query = f"""
        WITH scoped AS (
            SELECT
                collector_instance_id,
                {dimension} AS group_key,
                count() AS event_count
            FROM raw_events
            {where}
            GROUP BY collector_instance_id, group_key
        ),
        totals AS (
            SELECT
                collector_instance_id,
                sum(event_count) AS total_count
            FROM scoped
            GROUP BY collector_instance_id
        )
        SELECT
            scoped.collector_instance_id,
            scoped.group_key,
            scoped.event_count,
            scoped.event_count / totals.total_count AS share_ratio
        FROM scoped
        INNER JOIN totals USING (collector_instance_id)
        ORDER BY scoped.collector_instance_id ASC, scoped.event_count DESC, scoped.group_key ASC
        """
        rows = self.client.query(query, parameters=params).result_rows
        return [
            CollectorBiasRow(
                collector_instance_id=row[0],
                group_key=row[1],
                event_count=row[2],
                share_ratio=float(row[3]),
            )
            for row in rows
        ]
