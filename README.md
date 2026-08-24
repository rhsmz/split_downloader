# SplitDownloader

高性能・多機能な分割ダウンロードマネージャー（Rust + egui）。

ローカル起動のデスクトップアプリケーションとして設計されています。

## 実装済み機能

### 必須級
- **レート制限**: グローバル / タスク単位（token-bucket 近似）
- **同時ダウンロード数・接続数制限**: 設定可能（セマフォで制御）
- **自動リトライ + 指数バックオフ**: ジッター付き
- **プロキシ対応**: HTTP / SOCKS5（reqwest Proxy）
- **カスタムヘッダー / Cookie / User-Agent**
- **複数URL（ミラー）からの並列取得とフェイルオーバー**
- **ダウンロード完了後の自動リネーム（.part → 正式名）と一時ファイルクリーンアップ**
- **セキュアな一時ファイル**（Unix で 0600 権限）

### 高優先
- **バッチ / クリップボード監視**: クリップボードから URL を自動検出して追加ダイアログを開く
- **優先度キュー**: Critical / High / Normal（BinaryHeap）
- **適応的チャンクサイズ**: 設定で有効化（初期実装済み、動的調整は拡張ポイント）
- **帯域使用状況のリアルタイムグラフ**: egui_plot によるプレースホルダー（履歴収集を追加可能）
- **通知**: システム通知（notify-rust）+ 完了時コマンド実行
- **ログと診断情報のエクスポート**: タスク・チャンク状態をテキスト出力

### コア機能
- HTTP Range による分割並列ダウンロード
- 途中再開（チャンク状態保持、.part ファイル）
- ハッシュ検証（SHA-256 / MD5 / xxHash64）
- GUI でのチャンク状態可視化（色分け）
- クロスプラットフォーム設定ファイル（TOML + directories）

### 将来拡張ポイント（スタブ / 設計済み）
- BitTorrent / Magnet
- Metalink
- スケジュールダウンロード
- クラウドストレージ特有の対応
- 完全な SQLite 永続化（現在はメモリ + ファイル再開）

## 必要環境

- **Rust 1.80 以降**（edition 2021 + 最新依存クレートのため）
  - 現在のサンドボックスは 1.75 のため `cargo check` は失敗します。
  - ローカルで `rustup update` 後にビルドしてください。

## ビルドと実行

```bash
cd split_downloader
cargo build --release
cargo run --release
```

設定ファイルは OS 標準の config ディレクトリに保存されます：
- Linux: `~/.config/splitdownloader/settings.toml`
- macOS: `~/Library/Application Support/com.splitdownloader.SplitDownloader/settings.toml`
- Windows: `%APPDATA%\splitdownloader\SplitDownloader\config\settings.toml`

## アーキテクチャ概要

```
GUI (egui/eframe)
    ↕ コマンド / 進捗
DownloadManager
    - 優先度キュー
    - 同時タスク制限
    - グローバルレート制限
    ↓
TaskDownloader (1タスク)
    - HEAD / Range プローブ
    - チャンク分割
    - ワーカープール（接続数制限）
    - ミラーフェイルオーバー
    - リトライ
    - .part 書き込み + 最終リネーム
    - ハッシュ検証
```

## 主な設定項目（settings.toml）

- `max_concurrent_tasks`
- `max_connections_per_task`
- `global_rate_limit_bps` / `default_task_rate_limit_bps`
- `initial_chunk_size` / `min_chunk_size` / `max_chunk_size`
- `max_retries` / backoff
- `proxy`
- `user_agent`
- `enable_notifications`
- `on_complete_cmd`（`{path}` プレースホルダー）
- `clipboard_poll_ms`
- `adaptive_chunking`
- `secure_temp_files`

## 拡張のヒント

1. **完全な永続化**: `persistence/db.rs` に rusqlite でタスク・チャンクを保存し、起動時に復元。
2. **適応的チャンク**: リアルタイム速度を監視して `current_chunk_size` を動的に変更。
3. **帯域グラフ**: `DownloadManager` の `bandwidth_history` に定期的にポイントを追加し、GUI で描画。
4. **BitTorrent**: `librqbit` や `lava_torrent` などを別モジュールで統合。

このコードベースは実用的な基盤として動作するよう設計されています。必要に応じて個別モジュールを強化してください。
