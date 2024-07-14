from bigquery_timeseries.sql.basic import Query
from bigquery_timeseries.sql.resample import ResampleQuery
from bigquery_timeseries.uploader import Uploader
import pandas as pd

__version__ = "0.1.2"


class BQTS(Query, ResampleQuery):
    def __init__(self, project_id: str, dataset_id: str, *args, **kwargs):
        """
        Initialize the BigQuery TimeSeries class with project and dataset IDs.

        Parameters:
        project_id (str): Google Cloud project ID
        dataset_id (str): BigQuery dataset ID
        *args: Variable length argument list
        **kwargs: Arbitrary keyword arguments
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.uploader = Uploader(project_id, dataset_id)

    def upload(self, table_name: str, df: pd.DataFrame, mode: str = 'overwrite_partitions', dtypes: dict = None, schema: dict = None, use_gcs: bool = False, gcs_bucket_name: str = None, keep_gcs_file: bool = False, days_per_upload: int = None):
        """
        Upload data to BigQuery table with specified mode, data types, and schema.

        Parameters:
        table_name (str): Name of the table to upload data
        df (pd.DataFrame): DataFrame containing the data to upload
        mode (str): Upload mode ('overwrite_partitions' by default)
        dtypes (dict): Dictionary specifying the data types for the columns
        schema (dict): Dictionary specifying the schema for the table
        use_gcs (bool): Whether to use Google Cloud Storage for uploading (False by default)
        gcs_bucket_name (str): Name of the GCS bucket to use when use_gcs is True
        keep_gcs_file (bool): Whether to keep the GCS file after upload (False by default)
        days_per_upload (int): Number of days to group data for each upload (only used when use_gcs is False)
        """
        self.uploader.upload(
            table_name=table_name,
            df=df,
            dtype=dtypes,
            schema=schema,
            mode=mode,
            use_gcs=use_gcs,
            gcs_bucket_name=gcs_bucket_name,
            keep_gcs_file=keep_gcs_file,
            days_per_upload=days_per_upload
        )
