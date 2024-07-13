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
        dry_run: bool = False,
        type: str = "TIMESTAMP"  # Added type parameter
    ) -> Union[pd.DataFrame, Dict[str, Any]]:
        where_clauses = to_where(
            start_dt=start_dt,
            end_dt=end_dt,
            type=type,  # Pass the type parameter to the to_where function
            tz=tz
        )
        if symbols is not None and len(symbols) > 0:
            predicated = "\'" + "\',\'".join(symbols) + "\'"
            where_clauses += [f"symbol in ({predicated})"]

        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"

        if fields == '*':
            stmt = f"SELECT * FROM {table_id}"
        else:
            stmt = f"SELECT {', '.join(fields)}, symbol, dt FROM {table_id}"

        if len(where_clauses) > 0:
            stmt += " WHERE " + " AND ".join(where_clauses)

        stmt += f" ORDER BY symbol, dt"

        if dry_run:
            dry_run_query_job = self.bq_client.query(
                stmt, job_config=bigquery.QueryJobConfig(dry_run=True))
            return {
                "query": stmt,
                "total_bytes_processed": dry_run_query_job.total_bytes_processed,
                "total_bytes_billed": dry_run_query_job.total_bytes_billed,
            }

        query_job = self.bq_client.query(stmt)
        result = query_job.result()

        df = result.to_dataframe()

        return df

    def resample_query_with_confirmation(
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
        dry_run: bool = False,
        type: str = "TIMESTAMP"  # Added type parameter
    ) -> Union[pd.DataFrame, Dict[str, Any]]:
        dry_run_result = self.resample_query(
            table_name=table_name,
            fields=fields,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            interval=interval,
            tz=tz,
            ops=ops,
            where=where,
            cast=cast,
            verbose=verbose,
            offset_repr=offset_repr,
            dry_run=True,  # Always perform dry run first
            type=type  # Pass the type parameter to the resample_query function
        )

        if dry_run_result is not None:
            print(
                f"This query will process approximately {dry_run_result['total_bytes_processed'] / (1024 ** 3):.2f} GB of data.")
            print(
                f"The estimated cost is ${dry_run_result['total_bytes_billed'] / (1024 ** 3 * 5):.4f}.")
            confirm = input("Do you want to execute the query? (yes/no): ")
            if confirm.lower() != "yes":
                return None

        # Execute the actual query
        df = self.resample_query(
            table_name=table_name,
            fields=fields,
            symbols=symbols,
            start_dt=start_dt,
            end_dt=end_dt,
            interval=interval,
            tz=tz,
            ops=ops,
            where=where,
            cast=cast,
            verbose=verbose,
            offset_repr=offset_repr,
            dry_run=False,  # Actual run
            type=type  # Pass the type parameter to the resample_query function
        )

        return df
