from bitflyer_realtime_dashboard.clickhouse_client import build_alerts
from bitflyer_realtime_dashboard.models import CollectorStatus, FreshnessRow


def test_build_alerts_marks_stale_event_and_collector() -> None:
    freshness = [
        FreshnessRow(
            event_type="ticker",
            product_code="BTC_JPY",
            latest_received_at="2026-05-13T10:00:00",
            age_seconds=40,
        )
    ]
    collectors = [
        CollectorStatus(
            collector_instance_id="collector-a",
            total_rows=10,
            rows_1m=0,
            rows_15m=1,
            latest_received_at="2026-05-13T10:00:00",
            age_seconds=50,
        )
    ]

    alerts = build_alerts(freshness, collectors, base_stale_after_seconds=15)

    assert len(alerts) == 2
    assert alerts[0].severity == "critical"
    assert "stale" in alerts[0].message or "silent" in alerts[0].message
