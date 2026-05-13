# bitflyer-realtime-dashboard

ClickHouse に保存された bitFlyer realtime raw データを、terminal 上で見やすく監視するための Python 製 CLI ダッシュボードです。`raw_events` を中心に集計しつつ、`raw_tickers` の価格ミニチャートと `raw_board_snapshots` の板表示もあわせて出し、投資ツールらしい感覚で最新状態を追えるようにしています。

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
- 価格ミニチャート
  `raw_tickers` から product_code ごとの sparkline を表示
- executions パネル
  `raw_executions` から product_code ごとの直近価格レンジ、約定件数、合計サイズ、価格 sparkline を表示
- 板スナップショット表示
  `raw_board_snapshots` から asks / bids の上位レベルを常時表示
- 板差分表示
  `raw_board_deltas` から直近の asks / bids 変化レベルと mid / spread の変化量を表示
- alert パネル
  stale な `event_type x product_code` と沈黙した collector を強調表示
- dedupe 概況
  `count()` と `uniqExact(payload_hash)` の差分から一時重複を確認
- dedupe 表示切り替え
  `raw / unique / both` を切り替えて件数と throughput の見え方を変えられる
- collector 監視
  `collector_instance_id` ごとの age、直近件数、総件数を表示
- collector 偏り表示
  collector ごとの product_code 別件数と share を表示し、偏りを把握
- collector event_type 偏り表示
  collector ごとの event_type 別件数と share を表示し、channel 種別ごとの偏りを把握
- `since-minutes`
  直近何分を見るかを CLI オプションと TUI キーバインドで切り替え
- 閾値の設定化
  event_type ごとの stale 閾値と collector 無通信閾値を config で変更可能

## 対象テーブル

- `raw_events`
- `raw_executions`
- `raw_tickers`
- `raw_board_snapshots`
- `raw_board_deltas`

MVP では主に `raw_events` を読みます。`ReplacingMergeTree` の都合で、マージ完了前は一時的に重複行が見えることがあります。そのため Overview には raw 件数と `payload_hash` ベースの unique 件数の両方を表示します。

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
  ticker_stale_seconds: 15
  executions_stale_seconds: 30
  board_delta_stale_seconds: 30
  board_snapshot_stale_seconds: 150
  collector_stale_seconds: 30
  default_limit: 20
  chart_points: 30
  board_depth: 5
  dedupe_view: "both"
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
- `TICKER_STALE_SECONDS`
- `EXECUTIONS_STALE_SECONDS`
- `BOARD_DELTA_STALE_SECONDS`
- `BOARD_SNAPSHOT_STALE_SECONDS`
- `COLLECTOR_STALE_SECONDS`
- `DEDUPE_VIEW`

`.env` は `.gitignore` 済みです。

## 使い方

接続確認:

```bash
python -m bitflyer_realtime_dashboard doctor --config config.yaml
```

現在状態を一回だけ表示:

```bash
python -m bitflyer_realtime_dashboard snapshot --config config.yaml --since-minutes 1440
```

最新イベント一覧:

```bash
python -m bitflyer_realtime_dashboard latest --config config.yaml --limit 20 --since-minutes 1440
```

watch モード:

```bash
python -m bitflyer_realtime_dashboard watch --config config.yaml --refresh-seconds 2 --since-minutes 1440
```

unique ベースの件数表示:

```bash
python -m bitflyer_realtime_dashboard watch --config config.yaml --dedupe-view unique
```

TUI:

```bash
python -m bitflyer_realtime_dashboard tui --config config.yaml --since-minutes 1440
```

フィルタ:

```bash
python -m bitflyer_realtime_dashboard snapshot \
  --config config.yaml \
  --since-minutes 60 \
  --product-code BTC_JPY \
  --event-type ticker
```

## TUI の見どころ

- Overview
  総件数、直近 1 分 / 5 分 / 15 分件数、最終受信時刻、現在フィルタ、dedupe 概況
- Alerts
  stale な feed と沈黙した collector を一覧表示
- Market
  product_code ごとの最新価格、best bid / ask、spread、価格ミニチャート
- Collectors
  `collector_instance_id` ごとの age、1m 件数、15m 件数、総件数
- Collector Bias
  collector ごとの product_code 別件数と share を表示
- Collector Event Bias
  collector ごとの event_type 別件数と share を表示
- Executions
  product_code ごとの直近価格レンジ、約定件数、合計サイズ、価格ミニチャート
- Freshness
  `event_type x product_code` の鮮度を matrix で表示
- Board Snapshots
  各 product_code の asks / bids 上位レベルを terminal 上で常時表示
- Board Deltas
  各 product_code の直近 board delta で更新された価格帯、サイズ、mid / spread の変化量を表示
- Throughput
  系列別の件数と sparkline
- Latest Events
  直近イベントの一覧
- JSON Detail
  選択中イベントの raw JSON

## TUI キーバインド

- `q`
  終了
- `r`
  手動 refresh
- `1`
  直近 1 分表示へ切り替え
- `5`
  直近 5 分表示へ切り替え
- `f`
  直近 15 分表示へ切り替え
- `0`
  lookback 制限なし
- `d`
  `both -> raw -> unique` の順で dedupe view を切り替え

## 運用上の見方

- `Alerts` が空でない
  feed 停止や collector 停止の可能性があります
- `Dedupe` の `dup` が増える
  複数 collector の同時受信や Merge 前の一時重複が見えている可能性があります
- `dedupe=unique` にすると
  `payload_hash` ベースで一時重複を除いた件数に寄せて見られます
- `Collectors` で 1 台だけ `age` が大きい
  その collector が止まっているか、特定 run の ingest が止まっている可能性があります
- `Collector Bias` で share が極端に偏る
  collector ごとに購読や ingest の偏りが出ている可能性があります
- `Collector Event Bias` で board 系だけ偏る
  特定 event_type の購読や送信に偏りが出ている可能性があります
- `Executions` の trade 数や total size が急減する
  約定流量の変化、または executions 系 feed の遅延の可能性があります
- `Board Deltas` で asks / bids の差分が片側に偏る
  板の片側だけ激しく動いている局面を terminal 上でざっくり把握できます
- `Board Deltas` の `MID` や `Spread` 変化が大きい
  板中心価格や気配の開き方が急変している可能性があります

## ローカル ClickHouse と一緒に使う

既存の collector リポジトリ側で用意した ClickHouse に接続する想定です。ローカル ClickHouse が必要なら、collector 側の `docker-compose.clickhouse.yaml` を使うか、同等の ClickHouse を用意してください。

## テスト

```bash
python -m pytest
uv run ruff check .
```

## 今後の候補

- board_delta を使った板変化アニメーション風表示
- executions の buy / sell 比率表示
- board delta の size 変化をヒートマップ風に強調
