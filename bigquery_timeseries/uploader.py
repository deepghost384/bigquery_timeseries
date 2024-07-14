import io
import gzip
import time
import uuid
from typing import Any, Dict, Optional, Literal
import pandas as pd
from google.cloud import bigquery, storage
import pandas_gbq.schema
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console
from google.api_core.exceptions import NotFound
from loguru import logger

console = Console()

# Loguruの設定
logger.add("file_{time}.log", rotation="500 MB", level="DEBUG")


def upsert_table(
    project_id: str,
    dataset_id: str,
    bq_client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
    dtypes: Optional[Dict[str, str]] = None,
    schema: Optional[Dict[str, Any]] = None,
    mode: Literal['append', 'overwrite',
                  'overwrite_partitions'] = 'overwrite_partitions',
    days_per_upload: int = 1
) -> None:
    logger.info(
        f"Starting upsert_table for {project_id}.{dataset_id}.{table_name}")
    table_size = df.memory_usage(deep=True).sum()
    table_size_mb = table_size / (1024 * 1024)
    logger.info(f"Table size: {table_size_mb:.2f} MB")

    start_time = time.time()

    _dtypes = {
        "partition_dt": "datetime64[ns]",
        "dt": "datetime64[ns]",
        "symbol": "string",
    }

    if dtypes is not None:
        _dtypes.update(dtypes)

    logger.debug(f"Checking dtypes: {_dtypes}")
    for key, value in _dtypes.items():
        if key not in df.columns:
            error_msg = f"Column {key} must be given with dtype {value}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    logger.debug("Processing dataframe")
    df['dt'] = df['dt'].dt.tz_localize(None)
    df = df.astype(_dtypes)
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%d %H:%M:%S')

    logger.debug("Generating BigQuery schema")
    if schema is None:
        schema = pandas_gbq.schema.generate_bq_schema(df)
        schema = pandas_gbq.schema.remove_policy_tags(schema)
    bq_schema = pandas_gbq.schema.to_schema_fields(schema)

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        bq_client.get_table(table_id)
        table_exists = True
    except NotFound:
        table_exists = False

    if not table_exists or mode == 'overwrite':
        logger.info(f"Creating or overwriting table: {table_id}")
        table = bigquery.Table(table_id, schema=bq_schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="partition_dt",
            require_partition_filter=True
        )
        table.clustering_fields = ["symbol"]
        bq_client.create_table(table)

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND if mode == 'append' else bigquery.WriteDisposition.WRITE_TRUNCATE,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="partition_dt",
            require_partition_filter=True
        ),
        clustering_fields=["symbol"],
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10,
    )

    logger.info("Uploading data to BigQuery")
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    if job.error_result:
        error_msg = f"Job failed: {job.error_result}"
        logger.error(error_msg)
        raise Exception(error_msg)

    elapsed_time = time.time() - start_time
    logger.info(f"Table {table_id} updated in {elapsed_time:.2f} seconds")


class Uploader:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)

    def upload_to_gcs(self, gcs_bucket_name: str, df: pd.DataFrame) -> str:
        bucket = self.storage_client.bucket(gcs_bucket_name)
        blob_name = f"{uuid.uuid4()}.csv.gz"
        blob = bucket.blob(blob_name)

        logger.info(
            f"Uploading data to GCS bucket {gcs_bucket_name} as {blob_name}")

        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='w') as f:
            df.to_csv(f, index=False)
        buffer.seek(0)

        blob.upload_from_file(buffer, content_type='application/gzip')
        gcs_uri = f"gs://{gcs_bucket_name}/{blob_name}"
        logger.info(f"Data uploaded to GCS: {gcs_uri}")

        return gcs_uri

    def upload(self, table_name: str, df: pd.DataFrame, use_gcs: bool = False, gcs_bucket_name: Optional[str] = None, keep_gcs_file: bool = False):
        if use_gcs:
            if not gcs_bucket_name:
                raise ValueError(
                    "gcs_bucket_name must be provided when use_gcs is True")

            logger.info("Using GCS for BigQuery load")
            gcs_uri = self.upload_to_gcs(gcs_bucket_name, df)

            job_config = bigquery.LoadJobConfig(
                schema=pandas_gbq.schema.to_schema_fields(
                    pandas_gbq.schema.generate_bq_schema(df)),
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.MONTH,
                    field="partition_dt",
                    require_partition_filter=True
                ),
                clustering_fields=["symbol"],
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines=True,
                ignore_unknown_values=True,
                max_bad_records=10,
            )

            logger.info(f"Starting BigQuery load job from GCS: {gcs_uri}")
            job = self.bq_client.load_table_from_uri(
                gcs_uri,
                f"{self.project_id}.{self.dataset_id}.{table_name}",
                job_config=job_config
            )
            job.result()  # Wait for the job to complete

            if job.error_result:
                error_msg = f"Job failed: {job.error_result}"
                logger.error(error_msg)
                raise Exception(error_msg)

            if not keep_gcs_file:
                logger.info("Deleting temporary GCS file")
                bucket = self.storage_client.bucket(gcs_bucket_name)
                blob = bucket.blob(gcs_uri.split('/')[-1])
                blob.delete()
                logger.info("Temporary GCS file deleted")
            else:
                logger.info(f"GCS file kept at: {gcs_uri}")

            logger.info("Upload completed successfully")
        else:
            logger.info("Uploading data directly to BigQuery")
            upsert_table(
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                bq_client=self.bq_client,
                df=df,
                table_name=table_name
            )
