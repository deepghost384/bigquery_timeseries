# basic.py

from typing import Callable, Optional, List, Union, Dict, Any
import pandas as pd
from google.cloud import bigquery
from bigquery_timeseries.dt import (
    to_quarter_start_dt,
    to_month_start_dt,
    to_quarter_end_dt,
    to_month_end_dt,
)


def to_where(
    start_dt: Optional[str],
    end_dt: Optional[str],
    partition_key: str = "partition_dt",
    partition_interval: str = "quarterly",
    tz: Optional[str] = None
):
    start_dt_offset_fn: Callable
    end_dt_offset_fn: Callable

    if partition_interval == "quarterly":
        start_dt_offset_fn = to_quarter_start_dt
        end_dt_offset_fn = to_quarter_end_dt
    elif partition_interval == "monthly":
        start_dt_offset_fn = to_month_start_dt
        end_dt_offset_fn = to_month_end_dt
    else:
        raise ValueError(f"Invalid partition interval: {partition_interval}")

    where = []

    if start_dt is not None:
        _start_dt = pd.Timestamp(start_dt, tz=tz)
        if tz is not None:
            _start_dt = _start_dt.tz_convert("UTC").tz_localize(None)
        _start_dt = start_dt_offset_fn(_start_dt)

        where += [
            f"{partition_key} >= CAST('{_start_dt:%Y-%m-%d}' AS DATE)",
            f"dt >= CAST('{start_dt}' AS DATETIME)",
        ]

    if end_dt is not None:
        _end_dt = pd.Timestamp(end_dt, tz=tz)
        if tz is not None:
            _end_dt = _end_dt.tz_convert("UTC").tz_localize(None)
        _end_dt = end_dt_offset_fn(_end_dt)

        where += [
            f"{partition_key} <= CAST('{_end_dt:%Y-%m-%d}' AS DATE)",
            f"dt <= CAST('{end_dt}' AS DATETIME)",
        ]
    return where


class Query:
    def __init__(self, project_id: str, dataset_id: str, bq_client: bigquery.Client):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client

    def query(
        self,
        table_name: str,
        fields: Union[str, List[str]],
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        partition_key: str = "partition_dt",
        partition_interval: str = "quarterly",
        max_cost: float = 1.0  # 新しいパラメータ: 最大許容コスト（デフォルト1ドル）
    ) -> pd.DataFrame:
        try:
            # Ensure correct datetime format
            if start_dt is not None:
                start_dt = pd.Timestamp(start_dt).strftime('%Y-%m-%d %H:%M:%S')
            if end_dt is not None:
                end_dt = pd.Timestamp(end_dt).strftime('%Y-%m-%d %H:%M:%S')

            where = to_where(
                start_dt=start_dt,
                end_dt=end_dt,
                partition_key=partition_key,
                partition_interval=partition_interval,
            )

            if symbols is not None and len(symbols) > 0:
                predicated = "'" + "','".join(symbols) + "'"
                where += [f"symbol in ({predicated})"]

            table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

            if isinstance(fields, str) and fields == '*':
                stmt = f"SELECT * EXCEPT({partition_key}) FROM {table_id}"
            elif isinstance(fields, str):
                stmt = f"SELECT {fields}, symbol, dt FROM {table_id}"
            elif isinstance(fields, list):
                if 'symbol' not in fields:
                    fields.append('symbol')
                if 'dt' not in fields:
                    fields.append('dt')
                fields = [f for f in fields if f != partition_key]
                stmt = f"SELECT {','.join(fields)} FROM {table_id}"
            else:
                raise ValueError(
                    "Fields must be a string or a list of strings")

            if where:
                condition = " AND ".join(where)
                stmt += f" WHERE {condition}"

            # コスト見積もり
            job_config = bigquery.QueryJobConfig(
                dry_run=True, use_query_cache=False)
            dry_run_query_job = self.bq_client.query(
                stmt, job_config=job_config)
            bytes_processed = dry_run_query_job.total_bytes_processed
            estimated_cost = bytes_processed * 5 / 1e12  # $5 per TB

            print(
                f"This query will process approximately {bytes_processed / (1024 ** 3):.2f} GB of data.")
            print(f"The estimated cost is ${estimated_cost:.4f}.")

            if estimated_cost > max_cost:
                raise ValueError(
                    f"Estimated cost (${estimated_cost:.4f}) exceeds the maximum allowed cost (${max_cost:.2f}). Query execution cancelled.")

            df = pd.read_gbq(stmt, project_id=self.project_id,
                             use_bqstorage_api=True)

            # dt カラムが存在する場合、datetime に変換
            if 'dt' in df.columns:
                df["dt"] = pd.to_datetime(df["dt"])

            # dt カラムが存在する場合、dt をインデックスとして設定
            if 'dt' in df.columns:
                result = df.set_index("dt").sort_index()
            else:
                result = df

            # 重複列を削除
            if 'symbol_1' in result.columns:
                result = result.drop(columns=['symbol_1'])
            if 'dt_1' in result.columns:
                result = result.drop(columns=['dt_1'])

            # partition_dt カラムが残っている場合は削除
            if partition_key in result.columns:
                result = result.drop(columns=[partition_key])

            return result

        except Exception as e:
            print(f"An error occurred: {e}")
            print(f"Query: {stmt}")
            raise
