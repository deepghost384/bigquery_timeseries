from bigquery_timeseries.sql.basic import Query
from bigquery_timeseries.sql.resample import ResampleQuery
from google.cloud import bigquery

__version__ = "0.1.2"


class BQTS(Query, ResampleQuery):
    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(
            project=self.project_id, *args, **kwargs)
