# bigquery_timeseries

A custom library for working with BigQuery timeseries data.

## Install

```bash
pip install git+https://github.com/deepghost384/bigquery_timeseries.git -U
```

## Authentication

### Using gcloud command-line tool

If you're using the library on your local machine or in an environment where you can use the gcloud command-line tool, you can authenticate as follows:

```bash
gcloud auth application-default login
```

This command will open a web browser and prompt you to log in with your Google account. After successful authentication, you can use the bigquery_timeseries library without additional authentication steps in your Python code.

### Using in Google Colab environment

When using bigquery_timeseries in a Google Colab environment, perform authentication as follows:

```python
from google.colab import auth
from google.cloud import bigquery

# Authenticate in Colab environment
auth.authenticate_user()

# After authentication, proceed with the standard usage as shown below
```

## Usage

### Standard Usage

Here is an example to upload OHLC data:

```python
import bigquery_timeseries as bqts
import pandas as pd
import numpy as np

# Initialize Uploader
uploader = bqts.Uploader(
    project_id="your_project_id",
    dataset_id="your_dataset_id"
)

# Prepare example data
df = pd.DataFrame(np.random.randn(5000, 4))
df.columns = ['open', 'high', 'low', 'close']
df['symbol'] = np.random.choice(['BTCUSDT', 'ETHUSDT', 'BNBUSDT'], 5000)
df['dt'] = pd.date_range('2022-01-01', periods=5000, freq='15T')

# Set partition_dt for month partitioning
df['partition_dt'] = df['dt'].dt.date.map(lambda x: x.replace(day=1))

# Upload data directly to BigQuery
uploader.upload(
    table_name='example_table',
    df=df,
    mode='overwrite_partitions',  # Options: 'overwrite_partitions', 'append', 'overwrite'
    use_gcs=False,
    days_per_upload=7  # Upload 7 days of data at once
)

# Alternatively, upload data via Google Cloud Storage
uploader.upload(
    table_name='example_table',
    df=df,
    mode='overwrite_partitions',
    use_gcs=True,
    gcs_bucket_name='your-bucket-name',
    keep_gcs_file=False  # Set to True if you want to keep the temporary file in GCS
)
```

Here are examples to query data:

```python
import bigquery_timeseries as bqts

# Initialize BQTS client
bqts_client = bqts.BQTS(
    project_id="your_project_id",
    dataset_id="your_dataset_id"
)

# Standard query
result = bqts_client.query_with_confirmation(
    table_name='example_table',
    fields=['open', 'high', 'low', 'close'],
    start_dt='2022-02-01 00:00:00',
    end_dt='2022-02-05 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT'],
    type='STRING'
)
print(result.head(), "\nShape:", result.shape)

# Query to get all fields
result_all_fields = bqts_client.query_with_confirmation(
    table_name='example_table',
    fields=['*'],
    start_dt='2022-02-01 00:00:00',
    end_dt='2022-02-05 23:59:59',
    symbols=['BTCUSDT', 'ETHUSDT'],
    type='STRING'
)
print(result_all_fields.head(), "\nShape:", result_all_fields.shape)

# Resampling query
resampled_result = bqts_client.resample_query_with_confirmation(
    table_name='example_table',
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

## New Features

- Added support for uploading data via Google Cloud Storage (GCS) for improved performance with large datasets.
- Implemented progress tracking for data uploads.
- Enhanced error handling and logging.
- Added option to keep or delete temporary files in GCS after upload.
- **Removed support for day-level partitioning and retained month-level partitioning only.**

## Disclaimer

This library allows for potential SQL injection. Please use it for your own purposes only and do not allow arbitrary requests to this library.