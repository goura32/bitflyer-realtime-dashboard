from bitflyer_realtime_dashboard.clickhouse_client import (
    build_alerts,
    stale_threshold_for_event_type,
)
from bitflyer_realtime_dashboard.config import AppConfig
from bitflyer_realtime_dashboard.models import CollectorStatus, FreshnessRow


def test_build_alerts_marks_stale_event_and_collector() -> None:
    config = AppConfig.model_validate(
        {
            "dashboard": {
                "ticker_stale_seconds": 15,
                "collector_stale_seconds": 30,
            }
        }
    )
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

    alerts = build_alerts(config, freshness, collectors)

    assert len(alerts) == 2
    assert alerts[0].severity == "critical"
    assert "stale" in alerts[0].message or "silent" in alerts[0].message


def test_stale_threshold_for_event_type_uses_specific_config() -> None:
    config = AppConfig.model_validate(
        {
            "dashboard": {
                "stale_after_seconds": 15,
                "ticker_stale_seconds": 20,
                "executions_stale_seconds": 45,
                "board_delta_stale_seconds": 35,
                "board_snapshot_stale_seconds": 120,
            }
        }
    )

    assert stale_threshold_for_event_type(config, "ticker") == 20
    assert stale_threshold_for_event_type(config, "executions") == 45
    assert stale_threshold_for_event_type(config, "board_delta") == 35
    assert stale_threshold_for_event_type(config, "board_snapshot") == 120
    assert stale_threshold_for_event_type(config, "unknown") == 15
