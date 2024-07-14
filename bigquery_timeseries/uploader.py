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
    bq_schema = pandas_gbq.schema.to_google_cloud_bigquery(schema)

    new_bq_schema = []
    for field in bq_schema:
        if field.name == 'partition_dt':
            new_field = bigquery.SchemaField(
                name=field.name,
                field_type='DATE',
                mode=field.mode,
                description=field.description,
                fields=field.fields
            )
            new_bq_schema.append(new_field)
        else:
            new_bq_schema.append(field)

    bq_schema = new_bq_schema

    table_id = f"{project_id}.{dataset_id}.{table_name}"

    logger.debug(f"Setting up job configuration for mode: {mode}")
    if mode == 'append':
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif mode == 'overwrite':
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:  # 'overwrite_partitions'
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=write_disposition,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="partition_dt",
            require_partition_filter=True
        ),
        clustering_fields=["symbol"],
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10,
    )

    total_rows = len(df)
    uploaded_rows = 0

    logger.info(f"Starting upload process for {total_rows} rows")
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        upload_task = progress.add_task("Uploading data", total=total_rows)

        df_sorted = df.sort_values('partition_dt')
        date_groups = df_sorted.groupby(pd.Grouper(
            key='partition_dt', freq=f'{days_per_upload}D'))

        for _, group_df in date_groups:
            start_date = group_df['partition_dt'].min()
            end_date = group_df['partition_dt'].max()

            logger.debug(f"Processing date range: {start_date} to {end_date}")
            if (end_date - start_date).days >= days_per_upload:
                error_msg = f"Data for date range {start_date} to {end_date} exceeds the specified days_per_upload ({days_per_upload})."
                logger.error(error_msg)
                raise ValueError(error_msg)

            for partition_date in pd.date_range(start_date, end_date):
                partition_data = group_df[group_df['partition_dt'].dt.date == partition_date.date(
                )]

                if len(partition_data) == 0:
                    logger.debug(
                        f"No data for partition date: {partition_date}")
                    continue

                partition_date_str = partition_date.strftime('%Y%m%d')
                partition_table_id = f"{table_id}${partition_date_str}"

                logger.debug(
                    f"Preparing data for partition: {partition_date_str}")
                csv_buffer = io.StringIO()
                partition_data.to_csv(
                    csv_buffer, index=False, header=False, date_format='%Y-%m-%d')
                csv_buffer.seek(0)

                gzip_buffer = io.BytesIO()
                with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                    gz.write(csv_buffer.getvalue().encode())

                gzip_buffer.seek(0)

                logger.info(
                    f"Starting BigQuery load job for partition: {partition_date_str}")
                job = bq_client.load_table_from_file(
                    gzip_buffer,
                    partition_table_id,
                    job_config=job_config,
                )

                while not job.done():
                    time.sleep(1)
                    job.reload()
                    if job.state == 'RUNNING':
                        if job.output_rows is not None:
                            new_rows = job.output_rows - uploaded_rows
                            progress.update(
                                upload_task, advance=max(0, new_rows))
                            uploaded_rows = job.output_rows
                            logger.debug(
                                f"Uploaded {uploaded_rows} rows so far")

                if job.error_result:
                    error_msg = f"Job failed: {job.error_result}"
                    logger.error(error_msg)
                    raise Exception(error_msg)

                new_rows = len(partition_data)
                progress.update(upload_task, advance=new_rows)
                uploaded_rows += new_rows
                logger.info(
                    f"Completed upload for partition {partition_date_str}, total rows: {uploaded_rows}")

    end_time = time.time()
    upload_duration = end_time - start_time

    logger.info("Upload completed successfully")
    logger.info(f"Upload duration: {upload_duration:.2f} seconds")
    logger.info("Note: Partition filter requirement is enabled for this table.")


class Uploader:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        logger.info(
            f"Uploader initialized for project: {project_id}, dataset: {dataset_id}")

    def upload_to_gcs(self, bucket_name: str, df: pd.DataFrame) -> str:
        filename = f"temp_upload_{uuid.uuid4()}.csv.gz"
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        logger.info(f"Starting upload to GCS bucket: {bucket_name}")
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            with io.BytesIO() as gzip_buffer:
                with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                    gz.write(csv_buffer.getvalue().encode())
                gzip_buffer.seek(0)
                blob.upload_from_file(
                    gzip_buffer, content_type='application/gzip')

        gcs_uri = f"gs://{bucket_name}/{filename}"
        logger.info(f"Completed upload to GCS: {gcs_uri}")
        return gcs_uri

    def upload(
        self,
        table_name: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
        mode: Literal['append', 'overwrite',
                      'overwrite_partitions'] = 'overwrite_partitions',
        use_gcs: bool = False,
        gcs_bucket_name: Optional[str] = None,
        keep_gcs_file: bool = False,
        days_per_upload: Optional[int] = None
    ):
        logger.info(f"Starting upload process for table: {table_name}")
        if use_gcs:
            if gcs_bucket_name is None:
                error_msg = "gcs_bucket_name must be provided when use_gcs is True"
                logger.error(error_msg)
                raise ValueError(error_msg)
            logger.info("Using GCS for upload")
            self.upload_via_gcs(table_name, df, gcs_bucket_name,
                                dtype, schema, mode, keep_gcs_file)
        else:
            if days_per_upload is None:
                error_msg = "days_per_upload must be provided when use_gcs is False"
                logger.error(error_msg)
                raise ValueError(error_msg)
            logger.info("Using direct upload to BigQuery")
            self.upload_directly(table_name, df, dtype,
                                 schema, mode, days_per_upload)

    def upload_directly(
        self,
        table_name: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
        mode: Literal['append', 'overwrite',
                      'overwrite_partitions'] = 'overwrite_partitions',
        days_per_upload: int = 1
    ):
        logger.info(
            f"Starting direct upload to BigQuery for table: {table_name}")
        upsert_table(self.project_id, self.dataset_id, self.bq_client,
                     df, table_name, dtype, schema, mode, days_per_upload)

    def upload_via_gcs(
        self,
        table_name: str,
        df: pd.DataFrame,
        gcs_bucket_name: str,
        dtype: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
        mode: Literal['append', 'overwrite',
                      'overwrite_partitions'] = 'overwrite_partitions',
        keep_gcs_file: bool = False
    ):
        logger.info(f"Starting upload via GCS for table: {table_name}")
        _dtypes = {
            "partition_dt": "datetime64[ns]",
            "dt": "datetime64[ns]",
            "symbol": "string",
        }

        if dtype:
            _dtypes.update(dtype)

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
        bq_schema = pandas_gbq.schema.to_google_cloud_bigquery(schema)

        new_bq_schema = []
        for field in bq_schema:
            if field.name == 'partition_dt':
                new_field = bigquery.SchemaField(
                    name=field.name,
                    field_type='DATE',
                    mode=field.mode,
                    description=field.description,
                    fields=field.fields
                )
                new_bq_schema.append(new_field)
            else:
                new_bq_schema.append(field)

        bq_schema = new_bq_schema

        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        logger.debug(f"Setting up job configuration for mode: {mode}")
        if mode == 'append':
            write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif mode == 'overwrite':
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:  # 'overwrite_partitions'
            write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        job_config = bigquery.LoadJobConfig(
            schema=bq_schema,
            write_disposition=write_disposition,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="partition_dt",
                require_partition_filter=True
            ),
            clustering_fields=["symbol"],
            source_format=bigquery.SourceFormat.CSV,
            allow_quoted_newlines=True,
            ignore_unknown_values=True,
            max_bad_records=10,
        )

        logger.info("Uploading data to GCS")
        gcs_uri = self.upload_to_gcs(gcs_bucket_name, df)

        logger.info(f"Starting BigQuery load job from GCS: {gcs_uri}")
        job = self.bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
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
        logger.info("Note: Partition filter requirement is enabled for this table.")

        if __name__ == "__main__":
            logger.info("Starting main script execution")
            # ここにメインの実行コードを追加
            # 例:
            # project_id = "your-project-id"
            # dataset_id = "your-dataset-id"
            # table_name = "your-table-name"
            # uploader = Uploader(project_id, dataset_id)
            # df = pd.DataFrame(...)  # データフレームを作成または読み込み
            # uploader.upload(table_name, df, use_gcs=True, gcs_bucket_name="your-bucket-name")
            logger.info("Main script execution completed")