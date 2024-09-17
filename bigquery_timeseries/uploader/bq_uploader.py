import pandas as pd
from google.cloud import bigquery
from .base import BaseUploader
from .utils import check_query_cost

class BQUploader(BaseUploader):
    def load_to_bq(self, table_name: str, gcs_uri: str, df: pd.DataFrame, max_cost: float):
        table_id = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            schema=self.schema_manager.generate_schema(df),
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="partition_dt"
            ),
            clustering_fields=["symbol"],
            source_format=bigquery.SourceFormat.CSV,
            allow_quoted_newlines=True,
            ignore_unknown_values=False,
            max_bad_records=0,
            skip_leading_rows=1,
            autodetect=True,
        )

        self.log(f"Starting BigQuery load job from GCS: {gcs_uri}")
        self.log("Loading data into BigQuery...")
        load_job = self.bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=job_config
        )

        try:
            load_job.result()  # Wait for the job to complete
            self.log("BigQuery load complete.")
            self.log(f"Load job completed. Loaded {load_job.output_rows} rows.")
        except bigquery.BadRequest as e:
            self.log("BigQuery load failed.", level="ERROR")
            self.log(f"Load job failed with error: {e}", level="ERROR")
            for error in load_job.errors:
                self.log(f"Error details: {error}", level="ERROR")
            raise

        self.perform_final_check(table_name, df, max_cost)

    def perform_final_check(self, table_name: str, df: pd.DataFrame, max_cost: float):
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
                self.log(f"Final check - Total rows: {row.row_count}, Distinct symbols: {row.symbol_count}")
        except ValueError as e:
            self.log(f"Cost estimation error for final check query: {str(e)}", level="ERROR")
            self.log("Skipping final check due to cost estimation error", level="WARNING")