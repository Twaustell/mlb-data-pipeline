import sys
import logging

from ingestion.run_ingestion import ingest_date
from ingestion.transform import transform_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


def run_pipeline(date_str):
    logger.info(f"Starting pipeline for {date_str}")

    ingest_date(date_str)
    logger.info("Ingestion complete")

    transform_date(date_str)
    logger.info("Transformation complete")

    logger.info(f"Pipeline finished successfully for {date_str}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise ValueError("Usage: python pipeline.py YYYY-MM-DD")

    run_pipeline(sys.argv[1])