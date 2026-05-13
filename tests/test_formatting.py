from bitflyer_realtime_dashboard.formatting import (
    age_style,
    colored_flow_bar,
    flow_bar,
    heat_style,
    sparkline,
)


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


def test_colored_flow_bar_keeps_width() -> None:
    assert len(colored_flow_bar(3.0, 1.0).plain) == 12
    assert colored_flow_bar(0.0, 0.0).plain == "············"


def test_heat_style_uses_intensity_bands() -> None:
    assert heat_style(None, 10.0, "red") == "dim"
    assert heat_style(1.0, 10.0, "red") == "dim"
    assert heat_style(3.0, 10.0, "red") == "red dim"
    assert heat_style(6.0, 10.0, "red") == "red"
    assert heat_style(9.0, 10.0, "red") == "bold red"
