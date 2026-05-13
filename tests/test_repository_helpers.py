from bitflyer_realtime_dashboard.clickhouse_client import DashboardRepository
from bitflyer_realtime_dashboard.models import ThroughputRow


def test_throughput_by_series_without_client() -> None:
    repo = DashboardRepository.__new__(DashboardRepository)
    rows = [
        ThroughputRow("2026-05-13 10:00", "ticker", "BTC_JPY", 2),
        ThroughputRow("2026-05-13 10:01", "ticker", "BTC_JPY", 4),
        ThroughputRow("2026-05-13 10:00", "executions", "BTC_JPY", 1),
    ]
    series = DashboardRepository.throughput_by_series(repo, rows)
    assert series["ticker:BTC_JPY"] == [2, 4]
    assert series["executions:BTC_JPY"] == [1, 0]

