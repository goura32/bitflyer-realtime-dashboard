from bitflyer_realtime_dashboard.clickhouse_client import parse_board_snapshot


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
