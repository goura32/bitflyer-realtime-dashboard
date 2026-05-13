from pathlib import Path

from bitflyer_realtime_dashboard.config import load_config


def test_load_config_from_yaml_and_env(tmp_path: Path) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        """
clickhouse:
  host: "yaml-host"
dashboard:
  refresh_seconds: 5
""".strip()
        + "\n",
        encoding="utf-8",
    )
    env_path = tmp_path / ".env"
    env_path.write_text(
        (
            "CLICKHOUSE_HOST=env-host\n"
            "STALE_AFTER_SECONDS=30\n"
            "EXECUTIONS_STALE_SECONDS=45\n"
            "COLLECTOR_STALE_SECONDS=60\n"
        ),
        encoding="utf-8",
    )

    config = load_config(config_path=config_path, env_path=env_path)

    assert config.clickhouse.host == "env-host"
    assert config.dashboard.refresh_seconds == 5
    assert config.dashboard.stale_after_seconds == 30
    assert config.dashboard.executions_stale_seconds == 45
    assert config.dashboard.collector_stale_seconds == 60
