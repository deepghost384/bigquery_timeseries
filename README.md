# bigquery_timeseries

A custom library for working with BigQuery timeseries data.

## Install

```bash
pip install git+https://github.com/yourusername/bigquery_timeseries.git -U
```

## Usage

Here is an example to upload OHLC data.

```python
import bigquery_timeseries
import pandas as pd
import numpy as np
import boto3

# Set up AWS session
boto3_session = boto3.Session(
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Create AthenaTimeSeries object
tsdb = bigquery_timeseries.AthenaTimeSeries(
    boto3_session=boto3_session,
    glue_db_name=GLUE_DB_NAME,
    s3_path=S3_PATH
)

# Prepare example data, your data needs to have 3 columns named symbol, dt, partition_dt
df = pd.DataFrame(np.random.randn(5000, 4))
df.columns = ['open', 'high', 'low', 'close']

# symbol represents a group of data for given data columns
df['symbol'] = 'BTCUSDT'

# timestamp should be UTC timezone but without tz info
df['dt'] = pd.date_range('2022-01-01', '2022-05-01', freq='15Min')[:5000]

# partition_dt must be date, data will be updated partition by partition with use of this column.
# Every time, you have to upload all the data for a given partition_dt, otherwise older data will be gone.
df['partition_dt'] = df['dt'].dt.date.map(lambda x: x.replace(day=1))

tsdb.upload(table_name='example_table', df=df)
```

Here is an example to query data. You can enjoy time series resampling operations!

```python
# Query for raw data from 'example_table' for 'BTCUSDT' and 'ETHUSDT' symbols, retrieving 'open', 'high', 'low', 'close' fields
raw_close_open = tsdb.query(
    table_name='example_table',
    fields=['open','high','low','close'],
    start_dt='2022-02-01 00:00:00', # yyyy-mm-dd HH:MM:SS, inclusive
    end_dt='2022-02-05 23:59:59', # yyyy-mm-dd HH:MM:SS, inclusive
    symbols=['BTCUSDT','ETHUSDT'],
)

# Query for raw data from 'example_table' for 'BTCUSDT' and 'ETHUSDT' symbols, retrieving all fields
# Only tsdb.query supports fields=['*']
raw_close_open = tsdb.query(
    table_name='example_table',
    fields=['*'],
    start_dt='2022-02-01 00:00:00', # yyyy-mm-dd HH:MM:SS, inclusive
    end_dt='2022-02-05 23:59:59', # yyyy-mm-dd HH:MM:SS, inclusive
    symbols=['BTCUSDT','ETHUSDT'],
)

# Query for resampled data from 'example_table' for 'BTCUSDT' and 'ETHUSDT' symbols, retrieving 'open', 'high', 'low', 'close' fields on a daily interval
resampled_daily_close = tsdb.resample_query(
    table_name='example_table',
    fields=['open','high','low','close'],
    start_dt='2022-01-01 00:00:00', # yyyy-mm-dd HH:MM:SS, inclusive
    end_dt='2022-01-31 23:59:59', # yyyy-mm-dd HH:MM:SS, inclusive
    symbols=['BTCUSDT','ETHUSDT'],
    interval='day', # month | week | day | hour | {1,2,3,4,6,8,12}hour | minute | {5,15,30}minute
    ops=['first','max','min','last'], # last | first | min | max | sum
)
```

## Disclaimer

This allows you to have SQL injection. Please use it for your own purpose only and do not allow arbitrary requests to this library.