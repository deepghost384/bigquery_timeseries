from google.cloud import bigquery, storage
from ..logger import get_logger

class BaseUploader:
    def __init__(self, project_id: str, dataset_id: str):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        self.logger = get_logger(__name__)