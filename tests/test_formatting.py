from bitflyer_realtime_dashboard.formatting import age_style, flow_bar, sparkline


def test_sparkline_has_same_length() -> None:
    values = [0, 1, 2, 3, 4]
    result = sparkline(values)
    assert len(result) == len(values)


def test_age_style_thresholds() -> None:
    assert age_style(1, 10) == "green"
    assert age_style(6, 10) == "yellow"
    assert age_style(10, 10) == "red"
    assert age_style(None, 10) == "red"


def test_flow_bar_reflects_buy_sell_balance() -> None:
    assert flow_bar(0.0, 0.0) == "············"
    assert flow_bar(1.0, 0.0) == "████████████"
    assert flow_bar(0.0, 1.0) == "░░░░░░░░░░░░"
    assert len(flow_bar(3.0, 1.0)) == 12
