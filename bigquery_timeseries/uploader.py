# uploader.py

import io
import gzip
import time
import uuid
import csv
import pandas as pd
from google.cloud import bigquery, storage, exceptions as google_exceptions
import pandas_gbq
from rich.console import Console
from google.api_core.exceptions import BadRequest
from loguru import logger
from google.api_core import retry

console = Console()

logger.add("file_{time}.log", rotation="500 MB", level="DEBUG")


def check_query_cost(bq_client: bigquery.Client, query: str, max_cost: float = 1.0) -> None:
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = bq_client.query(query, job_config=job_config)

    bytes_processed = query_job.total_bytes_processed
    estimated_cost = bytes_processed * 5 / 1e12  # $5 per TB

    logger.debug(f"Estimated bytes processed: {bytes_processed:,} bytes")
    logger.debug(f"Estimated query cost: ${estimated_cost:.6f}")

    if estimated_cost > max_cost:
        raise ValueError(
            f"Estimated query cost (${estimated_cost:.6f}) exceeds the maximum allowed cost (${max_cost:.2f})")


class Uploader:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)

    @retry.Retry(predicate=retry.if_exception_type(
        google_exceptions.ServerError,
        google_exceptions.BadGateway,
        google_exceptions.ServiceUnavailable,
        google_exceptions.InternalServerError,
        google_exceptions.GatewayTimeout
    ))
    def upload_to_gcs_with_retry(self, bucket, blob, buffer):
        blob.upload_from_file(
            buffer, content_type='application/gzip', timeout=300)

    def upload_to_gcs(self, gcs_bucket_name: str, df: pd.DataFrame) -> str:
        bucket = self.storage_client.bucket(gcs_bucket_name)
        blob_name = f"{uuid.uuid4()}.csv.gz"
        blob = bucket.blob(blob_name)

        logger.debug(
            f"Uploading data to GCS bucket {gcs_bucket_name} as {blob_name}")

        logger.debug(
            f"Data sample being uploaded to GCS:\n{df.head().to_string()}")
        logger.debug(f"Data types:\n{df.dtypes}")

        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='w') as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_NONNUMERIC)
        buffer.seek(0)

        try:
            self.upload_to_gcs_with_retry(bucket, blob, buffer)
        except Exception as e:
            logger.error(f"Failed to upload to GCS after retries: {e}")
            raise

        gcs_uri = f"gs://{gcs_bucket_name}/{blob_name}"
        logger.debug(f"Data uploaded to GCS: {gcs_uri}")

        return gcs_uri

    def get_current_schema(self, table_id):
        try:
            table = self.bq_client.get_table(table_id)
            return table.schema
        except google_exceptions.NotFound:
            return None

    def compare_schemas(self, current_schema, new_schema):
        if current_schema is None:
            return False, new_schema

        current_fields = {field.name: field for field in current_schema}
        new_fields = {field['name']: field for field in new_schema}

        if set(current_fields.keys()) != set(new_fields.keys()):
            return True, new_schema

        for name, new_field in new_fields.items():
            current_field = current_fields[name]
            if current_field.field_type != new_field['type']:
                return True, new_schema

        return False, current_schema

    def update_table_schema(self, table_id, new_schema):
        table = self.bq_client.get_table(table_id)
        table.schema = new_schema
        self.bq_client.update_table(table, ['schema'])
        logger.debug(f"Updated schema for table {table_id}")

    def upload(self, table_name: str, df: pd.DataFrame, gcs_bucket_name: str, keep_gcs_file: bool = False, max_cost: float = 1.0):
        logger.debug(f"Starting upload process for table: {table_name}")
        logger.debug(f"Input DataFrame shape: {df.shape}")
        logger.debug(f"Input DataFrame columns: {df.columns.tolist()}")
        logger.debug(f"Input DataFrame dtypes:\n{df.dtypes}")

        # Ensure dt and partition_dt are in the correct format
        df['dt'] = pd.to_datetime(df['dt']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df['partition_dt'] = pd.to_datetime(
            df['partition_dt']).dt.strftime('%Y-%m-%d')

        logger.debug(f"DataFrame after initial processing:\n{df.head()}")
        logger.debug(
            f"DataFrame dtypes after initial processing:\n{df.dtypes}")

        schema = pandas_gbq.schema.generate_bq_schema(df)['fields']
        for field in schema:
            if field['name'] == 'partition_dt':
                field['type'] = 'DATE'
            elif field['name'] == 'dt':
                field['type'] = 'DATETIME'

        logger.debug(f"Generated schema: {schema}")

        logger.debug("Using GCS for BigQuery load")
        gcs_uri = self.upload_to_gcs(gcs_bucket_name, df)

        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        current_schema = self.get_current_schema(table_id)

        schema_changed, final_schema = self.compare_schemas(
            current_schema, schema)

        if current_schema is None:
            logger.debug(f"Table {table_id} not found. Creating a new table.")
            table = bigquery.Table(table_id, schema=final_schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="partition_dt",
                require_partition_filter=True
            )
            table.clustering_fields = ["symbol"]
            self.bq_client.create_table(table)
            logger.debug(f"Table {table_id} created successfully")
        elif schema_changed:
            logger.debug(
                f"Schema change detected for table {table_id}. Updating schema.")
            self.update_table_schema(table_id, final_schema)

        unique_partitions = df[['partition_dt', 'symbol']].drop_duplicates()
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
            logger.debug(f"Delete query: {delete_query}")
            try:
                check_query_cost(self.bq_client, delete_query, max_cost)
                delete_job = self.bq_client.query(delete_query)
                delete_job.result()
                logger.debug(
                    f"Deleted data for specified partition_dt and symbol combinations")
                logger.debug(
                    f"Rows affected: {delete_job.num_dml_affected_rows}")
            except google_exceptions.NotFound:
                logger.warning(
                    f"Table {table_id} not found. Skipping delete operation.")
            except ValueError as e:
                logger.error(f"Cost estimation error: {str(e)}")
                raise
            except Exception as e:
                logger.error(f"Error during delete operation: {str(e)}")
                raise

        job_config = bigquery.LoadJobConfig(
            schema=final_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
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

        logger.debug(f"Starting BigQuery load job from GCS: {gcs_uri}")
        job = self.bq_client.load_table_from_uri(
            gcs_uri,
            f"{self.project_id}.{self.dataset_id}.{table_name}",
            job_config=job_config,
            timeout=600
        )

        try:
            job.result()
            logger.debug(f"Job completed. Output rows: {job.output_rows}")
        except google_exceptions.BadRequest as e:
            logger.error(f"Job failed with error: {e}")
            for error in job.errors:
                logger.error(f"Error details: {error}")
            raise

        if not keep_gcs_file:
            logger.debug("Deleting temporary GCS file")
            bucket = self.storage_client.bucket(gcs_bucket_name)
            blob = bucket.blob(gcs_uri.split('/')[-1])
            blob.delete()
            logger.debug("Temporary GCS file deleted")
        else:
            logger.debug(f"GCS file kept at: {gcs_uri}")

        query = f"""
        SELECT COUNT(*) as row_count, COUNT(DISTINCT symbol) as symbol_count
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE partition_dt >= DATE('{df['partition_dt'].min()}')
          AND partition_dt <= DATE('{df['partition_dt'].max()}')
        """
        try:
            check_query_cost(self.bq_client, query, max_cost)
            query_job = self.bq_client.query(query)
            results = query_job.result()
            for row in results:
                logger.debug(
                    f"Final check - Total rows: {row.row_count}, Distinct symbols: {row.symbol_count}")
        except ValueError as e:
            logger.error(
                f"Cost estimation error for final check query: {str(e)}")
            logger.warning("Skipping final check due to cost estimation error")

        logger.debug("Upload process completed")
