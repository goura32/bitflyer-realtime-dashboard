from __future__ import annotations

EVENT_TYPE_STYLES = {
    "executions": "green",
    "ticker": "cyan",
    "board_snapshot": "yellow",
    "board_delta": "magenta",
}


def style_event_type(event_type: str) -> str:
    color = EVENT_TYPE_STYLES.get(event_type, "white")
    return f"[{color}]{event_type}[/{color}]"


def sparkline(values: list[int]) -> str:
    if not values:
        return ""
    blocks = "▁▂▃▄▅▆▇█"
    max_value = max(values)
    if max_value <= 0:
        return blocks[0] * len(values)
    return "".join(
        blocks[min(len(blocks) - 1, int(value / max_value * (len(blocks) - 1)))]
        for value in values
    )


def age_style(age_seconds: int | None, stale_after_seconds: int) -> str:
    if age_seconds is None:
        return "red"
    if age_seconds >= stale_after_seconds:
        return "red"
    if age_seconds >= stale_after_seconds // 2:
        return "yellow"
    return "green"
