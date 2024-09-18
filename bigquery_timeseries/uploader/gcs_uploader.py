# bigquery_timeseries/uploader/gcs_uploader.py

import io
import gzip
import uuid
import csv
import pandas as pd
from google.cloud import storage
from google.api_core import retry, exceptions
from .base import BaseUploader

class GCSUploader(BaseUploader):
    @retry.Retry(predicate=retry.if_exception_type(
        exceptions.ServerError,
        exceptions.BadGateway,
        exceptions.ServiceUnavailable,
        exceptions.InternalServerError,
        exceptions.GatewayTimeout
    ))
    def upload_to_gcs_with_retry(self, bucket, blob, buffer):
        self.logger.info(f"Attempting to upload blob: {blob.name}")
        blob.upload_from_file(buffer, content_type='application/gzip', timeout=300)
        self.logger.info(f"Successfully uploaded blob: {blob.name}")

    def upload_to_gcs(self, gcs_bucket_name: str, df: pd.DataFrame) -> str:
        self.logger.info(f"Starting upload to GCS bucket: {gcs_bucket_name}")
        self.logger.info(f"DataFrame shape: {df.shape}")

        bucket = self.storage_client.bucket(gcs_bucket_name)
        blob_name = f"{uuid.uuid4()}.csv.gz"
        blob = bucket.blob(blob_name)

        self.logger.info(f"Created blob with name: {blob_name}")

        buffer = io.BytesIO()

        # Compress data
        self.logger.info("Compressing data")
        with gzip.GzipFile(fileobj=buffer, mode='w') as f:
            df.to_csv(f, index=False, quoting=csv.QUOTE_NONNUMERIC)

        buffer.seek(0)

        # Upload to GCS
        try:
            self.logger.info("Attempting to upload to GCS")
            self.upload_to_gcs_with_retry(bucket, blob, buffer)
            self.logger.info("Upload to GCS completed successfully")
        except Exception as e:
            error_message = f"Failed to upload to GCS: {str(e)}"
            self.logger.error(error_message)
            raise

        gcs_uri = f"gs://{gcs_bucket_name}/{blob_name}"
        self.logger.info(f"Data uploaded to GCS: {gcs_uri}")

        return gcs_uri

    def delete_gcs_file(self, gcs_bucket_name: str, gcs_uri: str):
        self.logger.info("Deleting temporary GCS file")
        bucket = self.storage_client.bucket(gcs_bucket_name)
        blob = bucket.blob(gcs_uri.split('/')[-1])
        blob.delete()
        self.logger.info("Temporary GCS file deleted")