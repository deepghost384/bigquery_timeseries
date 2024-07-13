# bigquery_timeseries

A custom library for working with BigQuery timeseries data.

## Install

```bash
pip install git+https://github.com/yourusername/bigquery_timeseries.git -U
```

## Usage

Here is an example to upload OHLC data.

```python
import bigquery_timeseries as bqts
import pandas as pd
import numpy as np

# Initialize GCPTS client
bqts_client = bqts.GCPTS(
    project_id="your_project_id",
    dataset_id="your_dataset_id"
)

# Prepare example data
df = pd.DataFrame(np.random.randn(5000, 4))
df.columns = ['open', 'high', 'low', 'close']
df['symbol'] = np.random.choice(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'], 5000)
df['dt'] = pd.date_range('2022-01-01', periods=5000, freq='15T')
df['partition_dt'] = df['dt'].dt.floor('D')

# Upload data
bqts_client.upload(
    table_name='example_table',
    df=df,
    mode='overwrite_partitions'  # Options: 'overwrite_partitions', 'append', 'overwrite'
)
```

Here are examples to query data:

```python
import bigquery_timeseries as bqts

# Initialize GCPTS client
bqts_client = bqts.GCPTS(
    project_id="your_project_id",
    dataset_id="your_dataset_id"
)

# Standard query
result = bqts_client.query_with_confirmation(
    table_name='example_table4',
    fields=['open', 'high', 'low', 'close'],
    start_dt='2022-02-01 00:00:00',
    end_dt='2022-02-05 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT'],
    type='STRING'
)
print(result.head(), "\nShape:", result.shape)

# Query to get all fields
result_all_fields = bqts_client.query_with_confirmation(
    table_name='example_table4',
    fields=['*'],
    start_dt='2022-02-01 00:00:00',
    end_dt='2022-02-05 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT'],
    type='STRING'
)
print(result_all_fields.head(), "\nShape:", result_all_fields.shape)

# Resampling query
resampled_result = bqts_client.resample_query_with_confirmation(
    table_name='example_table4',
    fields=['open', 'high', 'low', 'close'],
    start_dt='2022-01-01 00:00:00',
    end_dt='2022-01-31 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT'],
    interval='day',
    ops=['first', 'max', 'min', 'last'],
    type='STRING'
)
print(resampled_result.head(), "\nShape:", resampled_result.shape)
```

## Disclaimer

This allows you to have SQL injection. Please use it for your own purpose only and do not allow arbitrary requests to this library.