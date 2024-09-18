from .uploader import Uploader
from .sql.basic import Query
from .sql.resample import ResampleQuery
import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm
from .logger import get_logger

logger = get_logger(__name__)

class BQTS:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.uploader = Uploader(project_id, dataset_id)
        self.query_client = Query(project_id, dataset_id, self.bq_client)
        self.resample_query_client = ResampleQuery(
            project_id, dataset_id, self.bq_client)

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