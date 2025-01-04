# 🚀 bigquery_timeseries

A powerful custom library for seamlessly working with BigQuery timeseries data.

## 📦 Install

```bash
pip install git+https://github.com/deepghost384/bigquery_timeseries.git -U
```

## 🔐 Authentication

### 🖥️ Using gcloud command-line tool

ローカル環境や gcloud CLI が導入されている環境では下記コマンドで認証します。

```bash
gcloud auth application-default login
```

認証に成功すると、Python コード内で特別な設定なしに本ライブラリを使用できます。

### 🧪 Using in Google Colab environment

Google Colab を使用する場合:

```python
from google.colab import auth
from google.cloud import bigquery

# Colab での認証
auth.authenticate_user()

# 認証後は通常どおりライブラリを利用可能
```

## 🚀 Usage

### 🔧 Initializing BQTS Client

```python
import bigquery_timeseries as bqts

bqts_client = bqts.BQTS(
   project_id="your_project_id",
   dataset_id="your_dataset_id"
)
```

### 📊 Uploading Data

OHLC データをアップロードする例:

```python
import pandas as pd
import numpy as np

# データのサンプルを作成
df = pd.DataFrame(np.random.randn(5000, 4))
df.columns = ['open', 'high', 'low', 'close']
df['symbol'] = np.random.choice(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'], 5000)
df['dt'] = pd.date_range('2022-01-01', periods=5000, freq='15T')

# テーブルの月間パーティション用に partition_dt を設定
df['partition_dt'] = df['dt'].dt.date.map(lambda x: x.replace(day=1))

# Google Cloud Storage 経由でアップロード
bqts_client.upload(
   table_name='example_table',
   df=df,
   gcs_bucket_name='your-bucket-name',
   keep_gcs_file=False,  # True にすると GCS 上の一時ファイルを保持
   max_cost=1.0
)
```



### 🔄 Upload Mode

`upload` メソッドは以下のステップで動作します:

1. 新規データの中から `partition_dt` と `symbol` のユニークな組み合わせを抽出  
2. 対応する既存データを一括削除 (`DELETE`)  
3. 新規データを `WRITE_APPEND` で追加挿入  

パーティション分割されているテーブルに対して、一部の期間・一部のシンボルだけを更新したいケースで役立ちます。また、実行コストが大きくなりすぎないよう、`max_cost` で上限を設定できます。



### 🔍 Querying Data

下記のようにデータを取得できます:

```python
# 通常のクエリ
result = bqts_client.query(
   table_name='example_table',
   fields=['open', 'high', 'low', 'close', 'symbol'],
   start_dt='2022-02-01 00:00:00',
   end_dt='2022-02-05 23:59:59',
   symbols=['BTCUSDT', 'ETHUSDT']
)
print(result.head(), "\nShape:", result.shape)

# すべてのカラムを取得
result_all_fields = bqts_client.query(
   table_name='example_table',
   fields=['*'],
   start_dt='2022-02-01 00:00:00',
   end_dt='2022-02-05 23:59:59',
   symbols=['BTCUSDT', 'ETHUSDT']
)
print(result_all_fields.head(), "\nShape:", result_all_fields.shape)

# リサンプリングクエリ
resampled_result = bqts_client.resample_query(
   table_name='example_table',
   fields=['open', 'high', 'low', 'close'],
   start_dt='2022-01-01 00:00:00',
   end_dt='2022-01-31 23:59:59',
   symbols=['BTCUSDT', 'ETHUSDT'],
   interval='day',
   ops=['first', 'max', 'min', 'last']
)
print(resampled_result.head(), "\nShape:", resampled_result.shape)
```

クエリ結果には `dt` カラムがインデックスとして設定され、`symbol` がカラムとして含まれます。パーティションに使用する `partition_dt` は結果に含まれません。



## 📝 Logging (Loguru の使用方法)

本ライブラリでは [Loguru](https://github.com/Delgan/loguru) を用いたログ出力を行います。  
ソースコードを見ると、`bigquery_timeseries/__init__.py` 内で `logger.remove()` が呼ばれているため、**デフォルトのログ出力先は削除**されています。  
そのため、**ユーザーが任意の出力先（コンソールやファイルなど）を改めて設定**する必要があります。

### 例: ログをファイルに出力する

```python
import bigquery_timeseries as bqts
from loguru import logger

# まずはデフォルトのハンドラを削除（bigquery_timeseries ではすでに remove 済）
logger.remove()

# ファイルにログを出力したい場合
logger.add("myapp.log", level="DEBUG", rotation="10 MB")

# コンソールにも INFO レベルで表示したい場合
# logger.add(sys.stderr, level="INFO")

# bigquery_timeseriesの出力は行わない場合
# logger.disable("bigquery_timeseries")

bqts_client = bqts.BQTS(
    project_id="your_project_id",
    dataset_id="your_dataset_id"
)

# 以降、ライブラリ呼び出しの際に、内部のログを含めて myapp.log に書き出されます
df = ...
bqts_client.upload(table_name="example_table", df=df, gcs_bucket_name="your-bucket")
```

## ⚠️ Disclaimer

本ライブラリのクエリ機能は、SQL インジェクションが起きうる実装部分を含んでいます。  
そのため、**「自分で用意したデータフレームやパラメータのみ」を扱う** ケースでのみ使用し、  
第三者が任意の入力を行えるような用途での使用は控えてください。

## 📄 License

このプロジェクトは [MIT License](LICENSE) でライセンスされています。