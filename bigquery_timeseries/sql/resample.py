# resample.py

from dataclasses import dataclass
from typing import List, Optional, Any, Dict, Union
import pandas as pd
from google.cloud import bigquery
from .basic import to_where


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
        return f"{self.field} like {self.value}"


@dataclass(frozen=True)
class Eq(Expr):
    field: str
    value: Any

    def to_repr(self) -> str:
        if isinstance(self.value, str):
            assert isinstance(self.value, str)
            return f"{self.field} = \'{self.value}\'"
        if isinstance(self.value, bool):
            str_value = "true" if self.value is True else "false"
            return f"{self.field} = {str_value}"
        if isinstance(self.value, int):
            return f"{self.field} = {self.value}"
        raise NotImplementedError


class ResampleQuery:
    def __init__(self, project_id: str, dataset_id: str, bq_client: bigquery.Client):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bq_client


    def resample_query(
        self,
        table_name: str,
        fields: List[str],
        symbols: Optional[List[str]] = None,
        start_dt: Optional[str] = None,
        end_dt: Optional[str] = None,
        interval: str = "day",
        tz: Optional[str] = None,
        ops: List[str] = ["last"],
        where: Optional[Expr] = None,
        cast: Optional[str] = None,
        verbose: int = 0,
        offset_repr: Optional[str] = None,
        max_cost: float = 1.0
    ) -> pd.DataFrame:
        where_clauses = to_where(
            start_dt=start_dt,
            end_dt=end_dt,
            partition_key="partition_dt",
            partition_interval="monthly",
            tz=tz
        )
        if symbols is not None and len(symbols) > 0:
            predicated = "\'" + "\',\'".join(symbols) + "\'"
            where_clauses += [f"symbol in ({predicated})"]

        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        # リサンプリングのための SQL を構築
        agg_functions = {
            "first": "FIRST_VALUE",
            "last": "LAST_VALUE",
            "max": "MAX",
            "min": "MIN",
            "sum": "SUM",
        }

        select_clauses = ["DATE(dt) AS dt"]  # 'dt' カラムを明示的に選択
        for field in fields:
            for op in ops:
                if op == 'first':
                    select_clauses.append(
                        f"FIRST_VALUE({field}) OVER(PARTITION BY symbol, DATE(dt) ORDER BY dt) AS {field}_{op}")
                elif op == 'last':
                    select_clauses.append(
                        f"LAST_VALUE({field}) OVER(PARTITION BY symbol, DATE(dt) ORDER BY dt ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS {field}_{op}")
                elif op in ['max', 'min', 'sum']:
                    select_clauses.append(f"{op.upper()}({field}) AS {field}_{op}")

        stmt = f"""
        WITH daily_data AS (
            SELECT 
                symbol,
                {', '.join(select_clauses)}
            FROM {table_id}
            WHERE {' AND '.join(where_clauses)}
        )
        SELECT 
            symbol,
            dt,
            {', '.join([f'{field}_{op}' for field in fields for op in ops])}
        FROM daily_data
        GROUP BY symbol, dt
        ORDER BY symbol, dt
        """

        # コスト見積もり
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        dry_run_query_job = self.bq_client.query(stmt, job_config=job_config)
        bytes_processed = dry_run_query_job.total_bytes_processed
        estimated_cost = bytes_processed * 5 / 1e12  # $5 per TB

        print(
            f"This query will process approximately {bytes_processed / (1024 ** 3):.2f} GB of data.")
        print(f"The estimated cost is ${estimated_cost:.4f}.")

        if estimated_cost > max_cost:
            raise ValueError(
                f"Estimated cost (${estimated_cost:.4f}) exceeds the maximum allowed cost (${max_cost:.2f}). Query execution cancelled.")

        query_job = self.bq_client.query(stmt)
        result = query_job.result()

        df = result.to_dataframe()
        df['dt'] = pd.to_datetime(df['dt'])
        df = df.set_index('dt').sort_index()

        return df
