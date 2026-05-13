from bitflyer_realtime_dashboard.clickhouse_client import (
    enrich_board_delta_views,
    parse_board_delta,
    parse_board_snapshot,
)


def test_parse_board_snapshot_extracts_levels() -> None:
    payload = """
    {
      "mid_price": 100.5,
      "bids": [{"price": 100.0, "size": 1.2}],
      "asks": [{"price": 101.0, "size": 0.8}]
    }
    """
    board = parse_board_snapshot(payload, "BTC_JPY", "2026-05-13 10:00:00", depth=5)

    assert board.product_code == "BTC_JPY"
    assert board.mid_price == 100.5
    assert board.bids[0].price == 100.0
    assert board.asks[0].size == 0.8


def test_parse_board_delta_extracts_levels() -> None:
    payload = """
    {
      "mid_price": 200.5,
      "bids": [{"price": 200.0, "size": 0.4}],
      "asks": [{"price": 201.0, "size": 0.6}]
    }
    """
    board = parse_board_delta(payload, "FX_BTC_JPY", "2026-05-13 10:01:00", depth=5)

    assert board.product_code == "FX_BTC_JPY"
    assert board.mid_price == 200.5
    assert board.best_bid == 200.0
    assert board.best_ask == 201.0
    assert board.spread == 1.0
    assert board.bids[0].size == 0.4
    assert board.asks[0].price == 201.0


def test_enrich_board_delta_views_sets_previous_mid_and_spread() -> None:
    latest = parse_board_delta(
        """
        {
          "mid_price": 205.0,
          "bids": [{"price": 204.0, "size": 0.5}],
          "asks": [{"price": 206.5, "size": 0.7}]
        }
        """,
        "BTC_JPY",
        "2026-05-13 10:02:00",
        depth=5,
    )
    previous = parse_board_delta(
        """
        {
          "mid_price": 203.0,
          "bids": [{"price": 202.0, "size": 0.5}],
          "asks": [{"price": 204.0, "size": 0.7}]
        }
        """,
        "BTC_JPY",
        "2026-05-13 10:01:00",
        depth=5,
    )

    enriched = enrich_board_delta_views([latest, previous])

    assert enriched[0].previous_mid_price == 203.0
    assert enriched[0].spread == 2.5
    assert enriched[0].previous_spread == 2.0
    assert enriched[1].previous_mid_price is None
