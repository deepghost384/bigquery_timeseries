import io
import gzip
import time
from typing import Any, Dict, Optional, Literal
import pandas as pd
from google.cloud import bigquery
import pandas_gbq.schema
from google.api_core import exceptions
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn
from rich.console import Console

console = Console()


def upsert_table(
    project_id: str,
    dataset_id: str,
    bq_client: bigquery.Client,
    df: pd.DataFrame,
    table_name: str,
    dtypes: Optional[Dict[str, str]] = None,
    schema: Optional[Dict[str, Any]] = None,
    mode: Literal['append', 'overwrite',
                  'overwrite_partitions'] = 'overwrite_partitions'
) -> None:
    # テーブルサイズの計算
    table_size = df.memory_usage(deep=True).sum()
    table_size_mb = table_size / (1024 * 1024)  # バイトからメガバイトに変換
    console.print(f"Table size: {table_size_mb:.2f} MB")

    start_time = time.time()  # アップロード開始時間を記録

    _dtypes = {
        "partition_dt": "datetime64[ns]",
        "dt": "datetime64[ns]",
        "symbol": "string",
    }

    for key, value in _dtypes.items():
        if key not in df.columns:
            console.print(
                f"[bold red]Error:[/bold red] Column {key} must be given with dtype {value}")
            raise ValueError(f"Column {key} must be given with dtype {value}")

    if dtypes is not None:
        for k, v in dtypes.items():
            _dtypes[k] = v

    console.print("Converting data types:")
    console.print(df.dtypes)

    # タイムゾーン情報を削除
    df['dt'] = df['dt'].dt.tz_localize(None)
    df = df.astype(_dtypes)

    # Convert only 'dt' column to string for CSV output
    df['dt'] = df['dt'].dt.strftime('%Y-%m-%d %H:%M:%S')
    # Keep 'partition_dt' as datetime

    if schema is None:
        schema = pandas_gbq.schema.generate_bq_schema(df)
        schema = pandas_gbq.schema.remove_policy_tags(schema)
    bq_schema = pandas_gbq.schema.to_google_cloud_bigquery(schema)

    # Ensure 'partition_dt' is of type DATE in the schema
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

    # モードに応じてwrite_dispositionを設定
    if mode == 'append':
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    elif mode == 'overwrite':
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    else:  # 'overwrite_partitions'
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition=write_disposition,
        schema_update_options=[
            "ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="partition_dt",
            require_partition_filter=True  # パーティションフィルタ要件を有効化
        ),
        clustering_fields=["symbol"],
        source_format=bigquery.SourceFormat.CSV,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        max_bad_records=10,  # Allow some bad records
    )

    total_rows = len(df)
    uploaded_rows = 0

    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TextColumn("•"),
        TimeRemainingColumn(),
    ) as progress:
        upload_task = progress.add_task("Uploading data", total=total_rows)

        for (date, symbol), group_df in df.groupby([df["partition_dt"].dt.date, "symbol"]):
            partition_table_id = f"{table_id}${date.strftime('%Y%m%d')}"

            # CSV データを圧縮
            csv_buffer = io.StringIO()
            group_df.to_csv(csv_buffer, index=False,
                            header=False, date_format='%Y-%m-%d')
            csv_buffer.seek(0)

            gzip_buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=gzip_buffer, mode='wb') as gz:
                gz.write(csv_buffer.getvalue().encode())

            gzip_buffer.seek(0)

            job = bq_client.load_table_from_file(
                gzip_buffer,
                partition_table_id,
                job_config=job_config,
            )

            while not job.done():
                time.sleep(1)  # 1秒待機
                job.reload()
                if job.state == 'RUNNING':
                    if job.output_rows is not None:
                        new_rows = job.output_rows - uploaded_rows
                        progress.update(upload_task, advance=max(0, new_rows))
                        uploaded_rows = job.output_rows

            if job.error_result:
                raise Exception(f"Job failed: {job.error_result}")

            # 各パーティションのアップロード完了後に進捗を更新
            new_rows = len(group_df)
            progress.update(upload_task, advance=new_rows)
            uploaded_rows += new_rows

    end_time = time.time()  # アップロード終了時間を記録
    upload_duration = end_time - start_time  # アップロードにかかった時間を計算

    console.print("[bold green]Upload completed successfully[/bold green]")
    console.print(f"Upload duration: {upload_duration:.2f} seconds")
    console.print(
        "[bold yellow]Note:[/bold yellow] Partition filter requirement is enabled for this table.")

class Uploader:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)

    def upload(
        self,
        table_name: str,
        df: pd.DataFrame,
        dtype: Optional[Dict[str, str]] = None,
        schema: Optional[Dict[str, Any]] = None,
        mode: Literal['append', 'overwrite',
                      'overwrite_partitions'] = 'overwrite_partitions'
    ):
        upsert_table(self.project_id, self.dataset_id,
                     self.bq_client, df, table_name, dtype, schema, mode)
