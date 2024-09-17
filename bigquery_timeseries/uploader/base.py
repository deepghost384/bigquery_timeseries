from google.cloud import bigquery, storage
from loguru import logger

class BaseUploader:
    def __init__(self, project_id: str, dataset_id: str, verbose: bool = False):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.bq_client = bigquery.Client(project=project_id)
        self.storage_client = storage.Client(project=project_id)
        self.verbose = verbose
        self._configure_logger()

    def _configure_logger(self):
        if self.verbose:
            logger.add("file_{time}.log", rotation="500 MB", level="DEBUG")
        else:
            logger.remove()
            logger.add(lambda _: None)

    def log(self, message: str, level: str = "DEBUG"):
        if self.verbose:
            getattr(logger, level.lower())(message)