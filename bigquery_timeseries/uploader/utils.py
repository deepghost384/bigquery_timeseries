from google.cloud import bigquery
from ..logger import get_logger

logger = get_logger(__name__)

def check_query_cost(bq_client: bigquery.Client, query: str, max_cost: float = 1.0) -> None:
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    query_job = bq_client.query(query, job_config=job_config)

    bytes_processed = query_job.total_bytes_processed
    estimated_cost = bytes_processed * 5 / 1e12  # $5 per TB

    logger.info(f"Estimated bytes processed: {bytes_processed:,} bytes")
    logger.info(f"Estimated query cost: ${estimated_cost:.6f}")

    if estimated_cost > max_cost:
        logger.warning(f"Estimated query cost (${estimated_cost:.6f}) exceeds the maximum allowed cost (${max_cost:.2f})")
        raise ValueError(
            f"Estimated query cost (${estimated_cost:.6f}) exceeds the maximum allowed cost (${max_cost:.2f})")