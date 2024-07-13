from bigquery_timeseries.sql.basic import Query
from bigquery_timeseries.sql.resample import ResampleQuery
from google.cloud import bigquery
from bigquery_timeseries.uploader import upsert_table

__version__ = "0.1.2"


class BQTS(Query, ResampleQuery):
    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(
            project=self.project_id, *args, **kwargs)

    def upload(self, table_name: str, df: pd.DataFrame, mode: str = 'overwrite_partitions', dtypes: dict = None, schema: dict = None):
        upsert_table(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            bq_client=self.bq_client,
            df=df,
            table_name=table_name,
            mode=mode,
            dtypes=dtypes,
            schema=schema
        )
