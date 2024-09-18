from .base import BaseUploader
from .gcs_uploader import GCSUploader
from .bq_uploader import BQUploader
from .schema_manager import SchemaManager

class Uploader(BaseUploader):
    def __init__(self, project_id: str, dataset_id: str):
        super().__init__(project_id, dataset_id)
        self.schema_manager = SchemaManager(self.bq_client)
        self.gcs_uploader = GCSUploader(project_id, dataset_id)
        self.bq_uploader = BQUploader(project_id, dataset_id, self.schema_manager)

    def upload(self, table_name: str, df, gcs_bucket_name: str, keep_gcs_file: bool = False, max_cost: float = 1.0):
        gcs_uri = self.gcs_uploader.upload_to_gcs(gcs_bucket_name, df)
        self.bq_uploader.load_to_bq(table_name, gcs_uri, df, max_cost)
        if not keep_gcs_file:
            self.gcs_uploader.delete_gcs_file(gcs_bucket_name, gcs_uri)