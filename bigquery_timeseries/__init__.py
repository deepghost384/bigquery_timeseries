# __init__.py

from bigquery_timeseries.sql.basic import Query
from bigquery_timeseries.sql.resample import ResampleQuery
from bigquery_timeseries.uploader import Uploader
import pandas as pd
from typing import Literal
from google.cloud import bigquery

__version__ = "0.1.3"


class BQTS(Query, ResampleQuery):
    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.uploader = Uploader(project_id, dataset_id)

        # Call parent class initializers
        Query.__init__(self, project_id, dataset_id, self.bq_client)
        ResampleQuery.__init__(self, project_id, dataset_id, self.bq_client)

    def upload(self, table_name: str, df: pd.DataFrame, dtypes: dict = None, schema: dict = None, use_gcs: bool = False, gcs_bucket_name: str = None, keep_gcs_file: bool = False, partition_type: Literal['day', 'month'] = 'month'):
        self.uploader.upload(
            table_name=table_name,
            df=df,
            use_gcs=use_gcs,
            gcs_bucket_name=gcs_bucket_name,
            keep_gcs_file=keep_gcs_file,
            partition_type=partition_type
        )
