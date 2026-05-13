# bitflyer-realtime-dashboard

ClickHouse に保存された bitFlyer realtime raw データを、terminal 上で見やすく監視するための Python 製 CLI ダッシュボードです。`raw_events` を中心に集計し、最新受信状況、鮮度、流量、最新イベントの raw JSON を確認できます。

## 機能

- `doctor`
  ClickHouse 接続確認と必要テーブルの存在確認
- `snapshot`
  現在状態を一回だけ集計表示
- `latest`
  最新イベント一覧の表示
- `watch`
  軽量な watch モードで定期更新
- `tui`
  `Textual` ベースのフルスクリーン TUI

## 対象テーブル

- `raw_events`
- `raw_executions`
- `raw_tickers`
- `raw_board_snapshots`
- `raw_board_deltas`

MVP では主に `raw_events` を読みます。`ReplacingMergeTree` の都合で、マージ完了前は一時的に重複行が見えることがあります。

## セットアップ

Python 3.11 以上を使ってください。

```bash
git clone https://github.com/goura32/bitflyer-realtime-dashboard.git
cd bitflyer-realtime-dashboard
cp config.example.yaml config.yaml
cp .env.example .env
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e ".[dev]"
```

`pip` でも構いません。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

## 設定

`config.example.yaml`:

```yaml
clickhouse:
  host: "127.0.0.1"
  port: 8123
  database: "clickhouse"
  username: "clickhouse"
  password: ""

dashboard:
  refresh_seconds: 2.0
  stale_after_seconds: 15
  default_limit: 20
  product_codes: []
  event_types: []
  channels: []
```

環境変数でも上書きできます。

- `CLICKHOUSE_HOST`
- `CLICKHOUSE_PORT`
- `CLICKHOUSE_DATABASE`
- `CLICKHOUSE_USERNAME`
- `CLICKHOUSE_PASSWORD`
- `REFRESH_SECONDS`
- `STALE_AFTER_SECONDS`

`.env` は `.gitignore` 済みです。

## 使い方

接続確認:

```bash
python -m bitflyer_realtime_dashboard doctor --config config.yaml
```

現在状態を一回だけ表示:

```bash
python -m bitflyer_realtime_dashboard snapshot --config config.yaml
```

最新イベント一覧:

```bash
python -m bitflyer_realtime_dashboard latest --config config.yaml --limit 20
```

watch モード:

```bash
python -m bitflyer_realtime_dashboard watch --config config.yaml --refresh-seconds 2
```

TUI:

```bash
python -m bitflyer_realtime_dashboard tui --config config.yaml
```

フィルタ:

```bash
python -m bitflyer_realtime_dashboard snapshot \
  --config config.yaml \
  --product-code BTC_JPY \
  --event-type ticker
```

## TUI の見どころ

- Overview
  総件数、直近 1 分 / 5 分 / 15 分件数、最終受信時刻
- Freshness
  `event_type x product_code` ごとの最新時刻と age seconds
- Throughput
  直近 15 分の系列別流量
- Latest Events
  直近イベントの一覧
- JSON Detail
  選択中イベントの raw JSON

## ローカル ClickHouse と一緒に使う

既存の collector リポジトリ側で用意した ClickHouse に接続する想定です。ローカル ClickHouse が必要なら、collector 側の `docker-compose.clickhouse.yaml` を使うか、同等の ClickHouse を用意してください。

## テスト

```bash
python -m pytest
uv run ruff check .
```

## 今後の候補

- 価格ミニチャート
- stale alert モード
- collector_instance_id 別の比較ビュー
- `payload_hash` ベースの疑似 dedupe 表示

