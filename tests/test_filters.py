from bitflyer_realtime_dashboard.clickhouse_client import build_where_clause
from bitflyer_realtime_dashboard.models import EventFilters


def test_build_where_clause_empty() -> None:
    where, params = build_where_clause(EventFilters())
    assert where == ""
    assert params == {}


def test_build_where_clause_with_filters() -> None:
    where, params = build_where_clause(
        EventFilters(
            product_codes=["BTC_JPY"],
            event_types=["ticker"],
            channels=["lightning_ticker_BTC_JPY"],
        )
    )
    assert "product_code IN %(product_codes)s" in where
    assert "event_type IN %(event_types)s" in where
    assert "channel IN %(channels)s" in where
    assert params["product_codes"] == ["BTC_JPY"]

