# uploader.py

import io
import gzip
import time
import uuid
import csv
from typing import Any, Dict, Optional, Literal
import pandas as pd
from google.cloud import bigquery, storage, exceptions as google_exceptions
import pandas_gbq
from rich.console import Console
from google.api_core.exceptions import BadRequest
from loguru import logger

console = Console()

# Loguruの設定
logger.add("file_{time}.log", rotation="500 MB", level="DEBUG")


def check_query_cost(bq_client: bigquery.Client, query: str, max_cost: float = 1.0) -> None:
    """
    クエリのコストを確認し、指定した最大コストを超える場合はエラーを発生させる
    """
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = bq_client.query(query, job_config=job_config)

    bytes_processed = query_job.total_bytes_processed
    estimated_cost = bytes_processed * 5 / 1e12  # $5 per TB

    logger.info(f"Estimated bytes processed: {bytes_processed:,} bytes")
    logger.info(f"Estimated query cost: ${estimated_cost:.6f}")

    if estimated_cost > max_cost:
        raise ValueError(
            f"Estimated query cost (${estimated_cost:.6f}) exceeds the maximum allowed cost (${max_cost:.2f})")


def upsert_table(
    project_id: str,
    dataset_id: str,
    bq_client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
    dtypes: Optional[Dict[str, str]] = None,
    schema: Optional[Dict[str, Any]] = None,
    partition_type: Literal['day', 'month'] = 'month',
    max_cost: float = 1.0
) -> None:
    logger.info(
        f"Starting upsert_table for {project_id}.{dataset_id}.{table_name}")
    logger.info(f"Input DataFrame shape: {df.shape}")
    logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
    logger.info(f"Input DataFrame dtypes:\n{df.dtypes}")
    logger.info(f"Partition type: {partition_type}")

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
    df['dt'] = pd.to_datetime(df['dt']).dt.tz_localize(None)
    df = df.astype(_dtypes)
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%d %H:%M:%S')

    # Adjust partitioning based on partition_type
    if partition_type == 'day':
        df['partition_dt'] = pd.to_datetime(df['dt']).dt.date
    else:  # Default to month
        df['partition_dt'] = pd.to_datetime(df['dt']).dt.to_period(
            'M').astype('datetime64[ns]').dt.date

    logger.info(f"DataFrame after processing:\n{df.head()}")
    logger.info(f"DataFrame dtypes after processing:\n{df.dtypes}")

    logger.debug("Generating BigQuery schema")
    if schema is None:
        schema = pandas_gbq.schema.generate_bq_schema(df)
        schema = pandas_gbq.schema.remove_policy_tags(schema)
        for field in schema['fields']:
            if field['name'] == 'partition_dt':
                field['type'] = 'DATE'  # Ensure partition_dt is of type DATE
    bq_schema = schema['fields']
    logger.info(f"Generated BigQuery schema: {bq_schema}")

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        bq_client.get_table(table_id)
        table_exists = True
        logger.info(f"Table {table_id} already exists")
    except BadRequest:
        table_exists = False
        logger.info(f"Table {table_id} does not exist")

    if not table_exists:
        logger.info(f"Creating table: {table_id}")
        table = bigquery.Table(table_id, schema=bq_schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY if partition_type == 'day' else bigquery.TimePartitioningType.MONTH,
            field="partition_dt",
            require_partition_filter=True
        )
        table.clustering_fields = ["symbol"]
        bq_client.create_table(table)
        logger.info(f"Table {table_id} created successfully")

    logger.info(
        "Preparing to delete existing data for specified partitions and symbols")
    unique_partitions = df[['partition_dt', 'symbol']].drop_duplicates()
    logger.info(
        f"Unique partition_dt and symbol combinations:\n{unique_partitions}")

    delete_conditions = []
    partition_dates = set()
    for _, row in unique_partitions.iterrows():
        condition = f"(partition_dt = DATE('{row['partition_dt']}') AND symbol = '{row['symbol']}')"
        delete_conditions.append(condition)
        partition_dates.add(row['partition_dt'])

    if delete_conditions:
        partition_filter = " OR ".join(
            [f"partition_dt = DATE('{date}')" for date in partition_dates])
        delete_query = f"""
        DELETE FROM `{table_id}`
        WHERE partition_dt IN (
            SELECT DISTINCT partition_dt
            FROM `{table_id}`
            WHERE {partition_filter}
        )
        AND ({" OR ".join(delete_conditions)})
        """
        logger.info(f"Delete query: {delete_query}")
        try:
            check_query_cost(bq_client, delete_query, max_cost)
            delete_job = bq_client.query(delete_query)
            delete_job.result()
            logger.info(
                f"Deleted data for specified partition_dt and symbol combinations")
            logger.info(f"Rows affected: {delete_job.num_dml_affected_rows}")
        except ValueError as e:
            logger.error(f"Cost estimation error: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error during delete operation: {str(e)}")
            raise

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY if partition_type == 'day' else bigquery.TimePartitioningType.MONTH,
            field="partition_dt",
            require_partition_filter=True
        ),
        clustering_fields=["symbol"],
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        ignore_unknown_values=False,
        max_bad_records=0,
    )

    logger.info("Uploading data to BigQuery")
    job = bq_client.load_table_from_dataframe(
        df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete

    if job.error_result:
        error_msg = f"Job failed: {job.error_result}"
        logger.error(error_msg)
        raise Exception(error_msg)

    logger.info(f"Job completed. Output rows: {job.output_rows}")

    elapsed_time = time.time() - start_time
    logger.info(f"Table {table_id} updated in {elapsed_time:.2f} seconds")

    # 最終確認: アップロードされたデータの確認
    query = f"""
    SELECT COUNT(*) as row_count, COUNT(DISTINCT symbol) as symbol_count
    FROM `{table_id}`
    WHERE partition_dt >= DATE('{df['partition_dt'].min()}')
      AND partition_dt <= DATE('{df['partition_dt'].max()}')
    """
    query_job = bq_client.query(query)
    results = query_job.result()
    for row in results:
        logger.info(
            f"Final check - Total rows: {row.row_count}, Distinct symbols: {row.symbol_count}")


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

        logger.info(
            f"Data sample being uploaded to GCS:\n{df.head().to_string()}")
        logger.info(f"Data types:\n{df.dtypes}")

        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='w') as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_NONNUMERIC)
        buffer.seek(0)

        blob.upload_from_file(buffer, content_type='application/gzip')
        gcs_uri = f"gs://{gcs_bucket_name}/{blob_name}"
        logger.info(f"Data uploaded to GCS: {gcs_uri}")

        return gcs_uri

    def upload(self, table_name: str, df: pd.DataFrame, use_gcs: bool = False, gcs_bucket_name: Optional[str] = None, keep_gcs_file: bool = False, partition_type: Literal['day', 'month'] = 'month', max_cost: float = 1.0):
        logger.info(f"Starting upload process for table: {table_name}")
        logger.info(f"Input DataFrame shape: {df.shape}")
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Input DataFrame dtypes:\n{df.dtypes}")

        df['dt'] = pd.to_datetime(df['dt']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['partition_dt'] = pd.to_datetime(
            df['partition_dt']).dt.strftime('%Y-%m-%d')

        object_columns = df.select_dtypes(include=['object']).columns
        for col in object_columns:
            df[col] = df[col].astype(str)

        logger.info(f"DataFrame after initial processing:\n{df.head()}")
        logger.info(f"DataFrame dtypes after initial processing:\n{df.dtypes}")

        schema = pandas_gbq.schema.generate_bq_schema(df)['fields']
        for field in schema:
            if field['name'] == 'partition_dt':
                field['type'] = 'DATE'
            elif field['name'] == 'dt':
                field['type'] = 'DATETIME'
            elif field['type'] == 'STRING':
                field['type'] = 'STRING'

        logger.info(f"Generated schema: {schema}")

        if use_gcs:
            if not gcs_bucket_name:
                raise ValueError(
                    "gcs_bucket_name must be provided when use_gcs is True")

            logger.info("Using GCS for BigQuery load")
            gcs_uri = self.upload_to_gcs(gcs_bucket_name, df)

            # Delete existing data for the specified partitions and symbols
            unique_partitions = df[[
                'partition_dt', 'symbol']].drop_duplicates()
            delete_conditions = []
            partition_dates = set()
            for _, row in unique_partitions.iterrows():
                condition = f"(partition_dt = DATE('{row['partition_dt']}') AND symbol = '{row['symbol']}')"
                delete_conditions.append(condition)
                partition_dates.add(row['partition_dt'])

            if delete_conditions:
                partition_filter = " OR ".join(
                    [f"partition_dt = DATE('{date}')" for date in partition_dates])
                delete_query = f"""
                DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                WHERE partition_dt IN (
                    SELECT DISTINCT partition_dt
                    FROM `{self.project_id}.{self.dataset_id}.{table_name}`
                    WHERE {partition_filter}
                )
                AND ({" OR ".join(delete_conditions)})
                """
                logger.info(f"Delete query: {delete_query}")
                try:
                    check_query_cost(self.bq_client, delete_query, max_cost)
                    delete_job = self.bq_client.query(delete_query)
                    delete_job.result()
                    logger.info(
                        f"Deleted data for specified partition_dt and symbol combinations")
                    logger.info(
                        f"Rows affected: {delete_job.num_dml_affected_rows}")
                except Exception as e:
                    logger.error(f"Error during delete operation: {str(e)}")
                    raise

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY if partition_type == 'day' else bigquery.TimePartitioningType.MONTH,
                    field="partition_dt",
                    require_partition_filter=True
                ),
                clustering_fields=["symbol"],
                source_format=bigquery.SourceFormat.CSV,
                allow_quoted_newlines=True,
                ignore_unknown_values=False,
                max_bad_records=0,
                skip_leading_rows=1,
                autodetect=True,
            )

            logger.info(f"Starting BigQuery load job from GCS: {gcs_uri}")
            job = self.bq_client.load_table_from_uri(
                gcs_uri,
                f"{self.project_id}.{self.dataset_id}.{table_name}",
                job_config=job_config
            )

            try:
                job.result()
                logger.info(f"Job completed. Output rows: {job.output_rows}")
            except google_exceptions.BadRequest as e:
                logger.error(f"Job failed with error: {e}")
                for error in job.errors:
                    logger.error(f"Error details: {error}")


                error_rows = self.bq_client.list_rows(
                    job.destination, selected_fields=job.schema, max_results=10)
                logger.error("Sample of error rows:")
                for row in error_rows:
                    logger.error(row)
                raise

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
            try:
                upsert_table(
                    project_id=self.project_id,
                    dataset_id=self.dataset_id,
                    bq_client=self.bq_client,
                    df=df,
                    table_name=table_name,
                    partition_type=partition_type,
                    max_cost=max_cost
                )
            except ValueError as e:
                logger.error(f"Upload failed due to cost estimation: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Upload failed with unexpected error: {str(e)}")
                raise

        # 最終確認: アップロードされたデータの確認
        query = f"""
        SELECT COUNT(*) as row_count, COUNT(DISTINCT symbol) as symbol_count
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE partition_dt >= DATE('{df['partition_dt'].min()}')
          AND partition_dt <= DATE('{df['partition_dt'].max()}')
        """
        query_job = self.bq_client.query(query)
        results = query_job.result()
        for row in results:
            logger.info(f"Final check - Total rows: {row.row_count}, Distinct symbols: {row.symbol_count}")

        logger.info("Upload process completed")
