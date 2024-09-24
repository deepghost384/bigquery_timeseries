# 🚀 bigquery_timeseries

A powerful custom library for seamlessly working with BigQuery timeseries data.

## 📦 Install

```bash
pip install git+https://github.com/deepghost384/bigquery_timeseries.git -U
```

## 🔐 Authentication

### 🖥️ Using gcloud command-line tool

For local machine or environments with gcloud CLI access:

```bash
gcloud auth application-default login
```

This command opens a web browser for Google account login. After successful authentication, you can use the bigquery_timeseries library without additional steps in your Python code.

### 🧪 Using in Google Colab environment

For Google Colab, authenticate as follows:

```python
from google.colab import auth
from google.cloud import bigquery

# Authenticate in Colab environment
auth.authenticate_user()

# Proceed with standard usage after authentication
```

## 🚀 Usage

### 🔧 Initializing BQTS Client

To use the bigquery_timeseries library, first initialize the BQTS client:

```python
import bigquery_timeseries as bqts

bqts_client = bqts.BQTS(
    project_id="your_project_id",
    dataset_id="your_dataset_id",
    verbose=False  # Set to True for verbose logging
)
```

### 📊 Uploading Data

Here's an example to upload OHLC data:

```python
import pandas as pd
import numpy as np

# Prepare example data
df = pd.DataFrame(np.random.randn(5000, 4))
df.columns = ['open', 'high', 'low', 'close']
df['symbol'] = np.random.choice(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'], 5000)
df['dt'] = pd.date_range('2022-01-01', periods=5000, freq='15T')

# Set partition_dt for month partitioning
df['partition_dt'] = df['dt'].dt.date.map(lambda x: x.replace(day=1))

# Upload data via Google Cloud Storage
bqts_client.upload(
    table_name='example_table',
    df=df,
    gcs_bucket_name='your-bucket-name',
    keep_gcs_file=False,  # Set to True to keep the temporary file in GCS
    max_cost=1.0
)
```

### 🔄 Upload Mode

The `upload` method uses a smart implementation combining targeted deletion and append operations:

1. Identifies unique combinations of `partition_dt` and `symbol` in the new data.
2. Constructs and executes a DELETE query to remove existing data for these specific combinations.
3. Appends the new data to the table using BigQuery's `WRITE_APPEND` disposition.

This approach allows for efficient updating of specific time periods and symbols without affecting other data in the table. It's particularly useful for scenarios where you need to update or replace data for certain date ranges and symbols while keeping the rest of the data intact.

The method also includes cost estimation checks to ensure that the operations don't exceed a specified cost threshold.

### 🔍 Querying Data

Here are examples to query data:

```python
# Standard query
result = bqts_client.query(
    table_name='example_table',
    fields=['open', 'high', 'low', 'close', 'symbol'],
    start_dt='2022-02-01 00:00:00',
    end_dt='2022-02-05 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT']
)
print(result.head(), "\nShape:", result.shape)

# Query to get all fields
result_all_fields = bqts_client.query(
    table_name='example_table',
    fields=['*'],
    start_dt='2022-02-01 00:00:00',
    end_dt='2022-02-05 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT']
)
print(result_all_fields.head(), "\nShape:", result_all_fields.shape)

# Resampling query
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

Note: Query results will have 'dt' as the index, and 'symbol' will be included as a regular column. The 'partition_dt' column is not included in the query results.

## 📝 Logging

The library uses Loguru for logging. By default, logs are written to a file with a 500 MB rotation. You can enable verbose logging by setting `verbose=True` when initializing the `BQTS` class.

## ⚠️ Disclaimer

This library allows for potential SQL injection. Please use it for your own purposes only and do not allow arbitrary requests to this library.

## 🎉 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License.