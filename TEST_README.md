# テスト実行ガイド

## 🛠️ セットアップ

### 1. 依存パッケージのインストール

```bash
pip install -e ".[dev]"
```

または、必要なテストライブラリを個別にインストール:

```bash
pip install pytest pytest-cov python-dotenv
```

### 2. 環境変数の設定

#### Windows PowerShell の場合

```powershell
# 一時的に環境変数を設定
$env:BQ_PROJECT_ID="your-project-id"
$env:BQ_DATASET_ID="your-dataset-id"
$env:GCS_BUCKET_NAME="your-bucket-name"

# または .env ファイルを使用（python-dotenv が必要）
```

#### Windows コマンドプロンプトの場合

```cmd
set BQ_PROJECT_ID=your-project-id
set BQ_DATASET_ID=your-dataset-id
set GCS_BUCKET_NAME=your-bucket-name
```

#### .env ファイルを使用する場合（推奨）

1. `.env.example` を `.env` にコピー
2. `.env` ファイルに実際の値を記入
3. テストコードで `python-dotenv` を使用して読み込み

```python
# テストコードの先頭に追加
from dotenv import load_dotenv
load_dotenv()
```

### 3. Google Cloud 認証

```bash
gcloud auth application-default login
```

## 🧪 テストの実行

### 全テストを実行

```bash
python -m pytest test/test_bigquery_timeseries.py -v
```

### 特定のテストクラスのみ実行

```bash
# アップロードテストのみ
pytest test/test_bigquery_timeseries.py::TestBQTSUpload -v

# クエリテストのみ
pytest test/test_bigquery_timeseries.py::TestBQTSQuery -v

# リサンプリングテストのみ
pytest test/test_bigquery_timeseries.py::TestBQTSResampleQuery -v

# 日付ユーティリティテストのみ
pytest test/test_bigquery_timeseries.py::TestBQTSDateUtils -v
```

### 特定のテスト関数のみ実行

```bash
pytest test/test_bigquery_timeseries.py::TestBQTSQuery::test_query_basic -v
```

### カバレッジレポート付きで実行

```bash
pytest test/test_bigquery_timeseries.py --cov=bigquery_timeseries --cov-report=html
```

HTMLレポートは `htmlcov/index.html` に生成されます。

### 詳細な出力で実行

```bash
pytest test/test_bigquery_timeseries.py -vv -s
```

## 📋 テストの構成

### TestBQTSUpload
- `test_upload_basic`: 基本的なアップロード機能
- `test_upload_with_partition_validation`: partition_dtのバリデーション
- `test_upload_incremental`: 増分アップロード（既存データの上書き）

### TestBQTSQuery
- `test_query_basic`: 基本的なクエリ
- `test_query_all_fields`: 全カラム取得
- `test_query_multiple_symbols`: 複数シンボルのクエリ
- `test_query_cost_limit`: クエリコスト制限の検証

### TestBQTSResampleQuery
- `test_resample_daily`: 日次リサンプリング
- `test_resample_hourly`: 時間リサンプリング
- `test_resample_weekly`: 週次リサンプリング
- `test_resample_invalid_ops`: 無効な操作の検証
- `test_resample_ops_mismatch`: 操作数の不一致検証

### TestBQTSDateUtils
- `test_is_date`: 日付判定
- `test_to_quarter`: 四半期計算
- `test_compute_monthly_intervals`: 月次区間計算

## ⚠️ 注意事項

1. **BigQueryとGCSへのアクセス権限が必要です**
   - テストは実際のBigQueryとGCSを使用します
   - 適切な権限を持つGCPプロジェクトが必要です

2. **テストテーブルについて**
   - テスト用テーブル `test_ohlc_data` が作成されます
   - テスト終了後、必要に応じて手動で削除してください

3. **コストについて**
   - BigQueryとGCSの使用料金が発生する可能性があります
   - テストは少量のデータを使用するよう設計されています
   - `max_cost` パラメータで制限をかけています

4. **認証情報の保護**
   - `.env` ファイルは `.gitignore` に追加してください
   - 環境変数に機密情報を含めないでください
   - GitHub等にpushする前に必ず確認してください

## 🔧 トラブルシューティング

### テストがスキップされる場合

```
SKIPPED [1] test/test_bigquery_timeseries.py:18: Environment variables BQ_PROJECT_ID and BQ_DATASET_ID must be set
```

→ 環境変数が正しく設定されているか確認してください

### 認証エラーが発生する場合

```
google.auth.exceptions.DefaultCredentialsError
```

→ `gcloud auth application-default login` を実行してください

### コスト制限エラーが発生する場合

```
ValueError: Estimated query cost exceeds the maximum allowed cost
```

→ テストコード内の `max_cost` パラメータを調整するか、より小さなデータ範囲でテストしてください

## 📊 継続的インテグレーション (CI)

GitHub Actions等でテストを実行する場合:

1. Secrets に環境変数を設定
2. GCPサービスアカウントキーを使用した認証
3. テスト環境専用のプロジェクト・データセットを使用

```yaml
# .github/workflows/test.yml の例
env:
  BQ_PROJECT_ID: ${{ secrets.BQ_PROJECT_ID }}
  BQ_DATASET_ID: ${{ secrets.BQ_DATASET_ID }}
  GCS_BUCKET_NAME: ${{ secrets.GCS_BUCKET_NAME }}
```