from .base import BaseUploader
from .gcs_uploader import GCSUploader
from .bq_uploader import BQUploader
from .schema_manager import SchemaManager

class Uploader(BaseUploader, GCSUploader, BQUploader):
    def __init__(self, project_id: str, dataset_id: str, verbose: bool = False):
        super().__init__(project_id, dataset_id, verbose)
        self.schema_manager = SchemaManager(self.bq_client)

    def upload(self, table_name: str, df, gcs_bucket_name: str, keep_gcs_file: bool = False, max_cost: float = 1.0):
        gcs_uri = self.upload_to_gcs(gcs_bucket_name, df)
        self.load_to_bq(table_name, gcs_uri, df, max_cost)
        if not keep_gcs_file:
            self.delete_gcs_file(gcs_bucket_name, gcs_uri)