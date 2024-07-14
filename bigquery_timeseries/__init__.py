from bigquery_timeseries.sql.basic import Query
from bigquery_timeseries.sql.resample import ResampleQuery
from bigquery_timeseries.uploader import Uploader
import pandas as pd

__version__ = "0.1.2"


class BQTS(Query, ResampleQuery):
    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.uploader = Uploader(project_id, dataset_id)

    def upload(self, table_name: str, df: pd.DataFrame, mode: str = 'overwrite_partitions', dtypes: dict = None, schema: dict = None, use_gcs: bool = False, gcs_bucket_name: str = None, keep_gcs_file: bool = False, days_per_upload: int = None, partition_type: str = 'day'):
        self.uploader.upload(
            table_name=table_name,
            df=df,
            partition_type=partition_type,
            dtype=dtypes,
            schema=schema,
            mode=mode,
            use_gcs=use_gcs,
            gcs_bucket_name=gcs_bucket_name,
            keep_gcs_file=keep_gcs_file,
            days_per_upload=days_per_upload
        )
