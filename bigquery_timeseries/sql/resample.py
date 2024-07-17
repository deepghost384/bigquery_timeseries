# resample.py

from dataclasses import dataclass
from typing import List, Optional, Any
import pandas as pd
from google.cloud import bigquery
from .basic import to_where
from loguru import logger
from .basic import Query

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
        fields: list,
        start_dt: str,
        end_dt: str,
        symbols: list = None,
        interval: str = "day",
        ops: list = None,
        partition_key: str = None,
        partition_interval: str = None,
        max_cost: float = 1.0,
    ):
        if ops is None:
            ops = ["first", "last", "min", "max", "mean"]

        interval_to_sql = {
            "day": "DATE(dt)",
            "week": "DATE_TRUNC(dt, WEEK)",
            "month": "DATE_TRUNC(dt, MONTH)",
            "quarter": "DATE_TRUNC(dt, QUARTER)",
            "year": "DATE_TRUNC(dt, YEAR)",
        }

        interval_sql = interval_to_sql.get(interval, "DATE(dt)")

        agg_functions = []
        for field in fields:
            for op in ops:
                if op == "first":
                    agg_functions.append(
                        f"ARRAY_AGG({field} IGNORE NULLS ORDER BY dt ASC LIMIT 1)[OFFSET(0)] AS {field}_{op}"
                    )
                elif op == "last":
                    agg_functions.append(
                        f"ARRAY_AGG({field} IGNORE NULLS ORDER BY dt DESC LIMIT 1)[OFFSET(0)] AS {field}_{op}"
                    )
                elif op == "mean":
                    agg_functions.append(f"AVG({field}) AS {field}_{op}")
                else:
                    agg_functions.append(
                        f"{op.upper()}({field}) AS {field}_{op}")

        agg_functions_str = ", ".join(agg_functions)

        query = f"""
        WITH daily_data AS (
            SELECT
                DATE(dt) AS dt, symbol, {agg_functions_str}
            FROM
                {self.project_id}.{self.dataset_id}.{table_name}
            WHERE
                partition_dt >= CAST('{start_dt}' AS DATE) AND
                dt >= CAST('{start_dt}' AS DATETIME) AND
                partition_dt <= CAST('{end_dt}' AS DATE) AND
                dt <= CAST('{end_dt}' AS DATETIME)
            {"AND symbol in (" + ', '.join([f"'{s}'" for s in symbols]) + ")" if symbols else ""}
            GROUP BY
                DATE(dt), symbol
        )
        SELECT
            *
        FROM
            daily_data
        ORDER BY
            dt, symbol
        """

        logger.info(f"Executing query: {query}")

        job_config = bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False)
        dry_run_query_job = self.bq_client.query(query, job_config=job_config)
        logger.info(
            f"This query will process {dry_run_query_job.total_bytes_processed} bytes"
        )

        estimated_cost = dry_run_query_job.total_bytes_processed * 5 / 1e12
        logger.info(f"Estimated cost: ${estimated_cost:.6f}")

        if estimated_cost > max_cost:
            raise ValueError(
                f"Estimated query cost (${estimated_cost:.6f}) exceeds the maximum allowed cost (${max_cost:.2f})"
            )

        query_job = self.bq_client.query(query)
        results = query_job.result()

        df = results.to_dataframe()
        df["dt"] = pd.to_datetime(df["dt"])
        df.set_index(["dt"], inplace=True)

        return df
