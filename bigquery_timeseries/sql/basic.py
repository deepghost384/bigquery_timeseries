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
        dry_run: bool = False
    ) -> Union[pd.DataFrame, Dict[str, Any]]:
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
                stmt = f"SELECT * FROM {table_id}"
            elif isinstance(fields, str):
                stmt = f"SELECT {fields} FROM {table_id}"
            elif isinstance(fields, list):
                # Remove 'dt' and 'symbol' if they're already in the fields list
                fields = [f for f in fields if f not in ['dt', 'symbol']]
                stmt = f"SELECT {','.join(fields)}, dt, symbol FROM {table_id}"
            else:
                raise ValueError(
                    "Fields must be a string or a list of strings")

            if where:
                condition = " AND ".join(where)
                stmt += f" WHERE {condition}"

            if dry_run:
                job_config = bigquery.QueryJobConfig(
                    dry_run=True, use_query_cache=False)
                dry_run_query_job = self.bq_client.query(
                    stmt, job_config=job_config)
                bytes_processed = dry_run_query_job.total_bytes_processed
                gb_processed = bytes_processed / (1024 * 1024 * 1024)
                estimated_cost = (gb_processed / 1024) * 5

                return {
                    "query": stmt,
                    "bytes_processed": bytes_processed,
                    "gb_processed": gb_processed,
                    "estimated_cost": estimated_cost
                }

            df = pd.read_gbq(stmt, project_id=self.project_id,
                             use_bqstorage_api=True)

            df["dt"] = pd.to_datetime(df["dt"])
            return df.set_index(["dt", "symbol"]).sort_index()

        except Exception as e:
            print(f"An error occurred: {e}")
            print(f"Query: {stmt}")
            raise

    def query_with_confirmation(
        self,
        table_name: str,
        fields: Union[str, List[str]],
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        partition_key: str = "partition_dt",
        partition_interval: str = "quarterly"
    ) -> Optional[pd.DataFrame]:
        dry_run_result = self.query(
            table_name, fields, symbols, start_dt, end_dt,
            partition_key, partition_interval, dry_run=True
        )

        print(
            f"This query will process approximately {dry_run_result['gb_processed']:.2f} GB of data.")
        print(
            f"The estimated cost is ${dry_run_result['estimated_cost']:.4f}.")

        user_input = input("Do you want to execute the query? (yes/no): ")

        if user_input.lower() == 'yes':
            return self.query(
                table_name, fields, symbols, start_dt, end_dt,
                partition_key, partition_interval
            )
        else:
            print("Query execution cancelled.")
            return None
