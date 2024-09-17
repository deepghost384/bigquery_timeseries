# __init__.py

from .uploader import Uploader
from .sql.basic import Query
from .sql.resample import ResampleQuery
from .logging import set_log_level
import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

class BQTS:
    def __init__(self, project_id: str, dataset_id: str, verbose: bool = False):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.uploader = Uploader(project_id, dataset_id, verbose)
        self.query_client = Query(project_id, dataset_id, self.bq_client)
        self.resample_query_client = ResampleQuery(
            project_id, dataset_id, self.bq_client)
        self.verbose = verbose

    def upload(self, table_name: str, df: pd.DataFrame, gcs_bucket_name: str, keep_gcs_file: bool = False, max_cost: float = 1.0):
        self.uploader.upload(
            table_name=table_name,
            df=df,
            gcs_bucket_name=gcs_bucket_name,
            keep_gcs_file=keep_gcs_file,
            max_cost=max_cost
        )

    def query(self, *args, **kwargs):
        return self.query_client.query(*args, **kwargs)

    def resample_query(self, *args, **kwargs):
        return self.resample_query_client.resample_query(*args, **kwargs)
<<<<<<< HEAD

    @staticmethod
    def set_log_level(level):
        set_log_level(level)
=======
>>>>>>> parent of 35f2f14 (Refactor uploader module for improved modularity and maintainability)
