from bitflyer_realtime_dashboard.clickhouse_client import (
    DashboardRepository,
    summarize_execution_payload_rows,
)
from bitflyer_realtime_dashboard.models import ThroughputRow


def test_throughput_by_series_without_client() -> None:
    repo = DashboardRepository.__new__(DashboardRepository)
    rows = [
        ThroughputRow("2026-05-13 10:00", "ticker", "BTC_JPY", 2, 1),
        ThroughputRow("2026-05-13 10:01", "ticker", "BTC_JPY", 4, 3),
        ThroughputRow("2026-05-13 10:00", "executions", "BTC_JPY", 1, 1),
    ]
    series = DashboardRepository.throughput_by_series(repo, rows)
    assert series["ticker:BTC_JPY"] == [2, 4]
    assert series["executions:BTC_JPY"] == [1, 0]

    unique_series = DashboardRepository.throughput_by_series(repo, rows, dedupe_view="unique")
    assert unique_series["ticker:BTC_JPY"] == [1, 3]


def test_summarize_execution_payload_rows_counts_buy_sell_and_size() -> None:
    summary = summarize_execution_payload_rows(
        "BTC_JPY",
        [
            (
                "2026-05-14 10:00:00",
                """
                [
                  {"side": "BUY", "price": 100.0, "size": 0.4},
                  {"side": "SELL", "price": 101.0, "size": 0.1}
                ]
                """,
            ),
            (
                "2026-05-14 10:00:01",
                """
                [
                  {"side": "BUY", "price": 102.0, "size": 0.3}
                ]
                """,
            ),
        ],
        points_per_product=30,
    )

    assert summary.trade_count == 3
    assert summary.buy_count == 2
    assert summary.sell_count == 1
    assert summary.buy_size == 0.7
    assert summary.sell_size == 0.1
    assert summary.total_size == 0.8
    assert summary.latest_price == 102.0
