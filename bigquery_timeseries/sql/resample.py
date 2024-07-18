# resample.py

from dataclasses import dataclass
from typing import List, Optional, Any
import pandas as pd
from google.cloud import bigquery
from .basic import to_where
from loguru import logger
from .basic import Query
import re
from dateutil.relativedelta import relativedelta

@dataclass(frozen=True)
class Expr:
    def to_repr(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class And(Expr):
    exprs: List[Expr]

    def to_repr(self) -> str:
        return " AND ".join([f"({item.to_repr()})" for item in self.exprs])


@dataclass(frozen=True)
class Or(Expr):
    exprs: List[Expr]

    def to_repr(self) -> str:
        return " OR ".join([f"({item.to_repr()})" for item in self.exprs])


@dataclass(frozen=True)
class GT(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} > {self.value}"


@dataclass(frozen=True)
class GTE(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} >= {self.value}"


@dataclass(frozen=True)
class LT(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} < {self.value}"


@dataclass(frozen=True)
class LTE(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, (int, float))
        return f"{self.field} <= {self.value}"


@dataclass(frozen=True)
class Like(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        assert isinstance(self.value, str)
        return f"{self.field} like '{self.value}'"


@dataclass(frozen=True)
class Eq(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        if isinstance(self.value, str):
            return f"{self.field} = '{self.value}'"
        if isinstance(self.value, bool):
            str_value = "true" if self.value is True else "false"
            return f"{self.field} = {str_value}"
        if isinstance(self.value, int):
            return f"{self.field} = {self.value}"
        raise NotImplementedError


class ResampleQuery(Query):
    def __init__(self, project_id: str, dataset_id: str, bq_client: bigquery.Client):
        super().__init__(project_id, dataset_id, bq_client)


    def resample_query(
        self,
        table_name: str,
        fields: List[str],
        ops: List[str],
        start_dt: str,
        end_dt: str,
        symbols: List[str] = None,
        interval: str = "1day",
        partition_key: str = "partition_dt",
        partition_interval: str = "quarterly",
        max_cost: float = 1.0,
    ):
        # Validate and process interval
        interval_sql, group_by_sql = self._process_interval(interval)

        # Validate operations
        valid_ops = ["last", "first", "min", "max", "sum"]
        if len(fields) != len(ops):
            raise ValueError(
                "The number of fields must match the number of operations")
        for op in ops:
            if op not in valid_ops:
                raise ValueError(
                    f"Invalid operation: {op}. Must be one of {valid_ops}")

        # Construct aggregation functions
        agg_functions = []
        for field, op in zip(fields, ops):
            if op == "last":
                agg_functions.append(
                    f"ARRAY_AGG({field} IGNORE NULLS ORDER BY dt DESC LIMIT 1)[OFFSET(0)] AS {field}"
                )
            elif op == "first":
                agg_functions.append(
                    f"ARRAY_AGG({field} IGNORE NULLS ORDER BY dt ASC LIMIT 1)[OFFSET(0)] AS {field}"
                )
            else:
                agg_functions.append(f"{op.upper()}({field}) AS {field}")

        agg_functions_str = ", ".join(agg_functions)

        # Construct WHERE clause
        where_conditions = to_where(
            start_dt=start_dt,
            end_dt=end_dt,
            partition_key=partition_key,
            partition_interval=partition_interval,
        )
        if symbols:
            symbols_str = ", ".join([f"'{s}'" for s in symbols])
            where_conditions.append(f"symbol IN ({symbols_str})")
        where_clause = " AND ".join(where_conditions)

        # Construct the query
        query = f"""
        WITH resampled_data AS (
            SELECT
                {interval_sql} AS dt,
                symbol,
                {agg_functions_str}
            FROM
                {self.project_id}.{self.dataset_id}.{table_name}
            WHERE
                {where_clause}
            GROUP BY
                {group_by_sql}, symbol
        )
        SELECT *
        FROM resampled_data
        ORDER BY dt, symbol
        """

        logger.debug(f"Executing query: {query}")

        # Estimate query cost
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_query_job = self.bq_client.query(query, job_config=job_config)
        estimated_cost = dry_run_query_job.total_bytes_processed * 5 / 1e12
        logger.debug(f"Estimated cost: ${estimated_cost:.6f}")

        if estimated_cost > max_cost:
            raise ValueError(
                f"Estimated query cost (${estimated_cost:.6f}) exceeds the maximum allowed cost (${max_cost:.2f})"
            )

        # Execute the query
        query_job = self.bq_client.query(query)
        results = query_job.result()

        # Convert to DataFrame
        df = results.to_dataframe()
        df["dt"] = pd.to_datetime(df["dt"])
        df.set_index(["dt"], inplace=True)

        return df


    def _process_interval(self, interval: str):
        interval_lower = interval.lower()
        match = re.match(
            r'^(\d+)?\s*(minute|hour|day|week|month|year)s?$', interval_lower)

        if not match:
            raise ValueError(f"Invalid interval: {interval}")

        number, unit = match.groups()
        number = int(number) if number else 1

        if unit == 'minute':
            if number == 1:
                return "DATETIME_TRUNC(dt, MINUTE)", "DATETIME_TRUNC(dt, MINUTE)"
            else:
                return (f"DATETIME_TRUNC(dt, MINUTE) - MOD(EXTRACT(MINUTE FROM dt), {number}) * INTERVAL 1 MINUTE",
                        f"DATETIME_TRUNC(dt, MINUTE) - MOD(EXTRACT(MINUTE FROM dt), {number}) * INTERVAL 1 MINUTE")
        elif unit == 'hour':
            if number == 1:
                return "DATETIME_TRUNC(dt, HOUR)", "DATETIME_TRUNC(dt, HOUR)"
            else:
                return (f"DATETIME_TRUNC(dt, HOUR) - MOD(EXTRACT(HOUR FROM dt), {number}) * INTERVAL 1 HOUR",
                        f"DATETIME_TRUNC(dt, HOUR) - MOD(EXTRACT(HOUR FROM dt), {number}) * INTERVAL 1 HOUR")
        elif unit == 'day':
            return f"DATE_TRUNC(dt, DAY) - MOD(DATE_DIFF(dt, DATE '1970-01-01', DAY), {number}) * INTERVAL 1 DAY", \
                f"DATE_TRUNC(dt, DAY) - MOD(DATE_DIFF(dt, DATE '1970-01-01', DAY), {number}) * INTERVAL 1 DAY"
        elif unit == 'week':
            return f"DATE_TRUNC(dt, WEEK) - MOD(DATE_DIFF(dt, DATE '1970-01-04', WEEK), {number}) * INTERVAL 7 DAY", \
                f"DATE_TRUNC(dt, WEEK) - MOD(DATE_DIFF(dt, DATE '1970-01-04', WEEK), {number}) * INTERVAL 7 DAY"
        elif unit == 'month':
            return f"DATE_TRUNC(dt, MONTH) - MOD(DATE_DIFF(dt, DATE '1970-01-01', MONTH), {number}) * INTERVAL 1 MONTH", \
                f"DATE_TRUNC(dt, MONTH) - MOD(DATE_DIFF(dt, DATE '1970-01-01', MONTH), {number}) * INTERVAL 1 MONTH"
        elif unit == 'year':
            return f"DATE_TRUNC(dt, YEAR) - MOD(DATE_DIFF(dt, DATE '1970-01-01', YEAR), {number}) * INTERVAL 1 YEAR", \
                f"DATE_TRUNC(dt, YEAR) - MOD(DATE_DIFF(dt, DATE '1970-01-01', YEAR), {number}) * INTERVAL 1 YEAR"
        else:
            raise ValueError(f"Unsupported interval unit: {unit}")
