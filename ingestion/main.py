"""
Main orchestrator for data ingestion pipeline.
Runs all ingestion scripts sequentially and then triggers dbt.
"""

import os
import sys
import subprocess
from pathlib import Path
from datetime import datetime
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Add the project root to the path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def run_ingestion_scripts():
    """
    Run all ingestion scripts from the sources directory.
    """
    logger.info("Starting data ingestion pipeline")

    # Import and run each source module
    from ingestion.sources import purchase_form_data
    from ingestion.sources import leads_ads_community
    from ingestion.sources import website_form_responses
    from ingestion.sources import leads_course_strategi_ads
    from ingestion.sources import branding_level_up

    sources = [
        ("purchase_form_data", purchase_form_data.ingest_purchase_data),
        ("leads_ads_community", leads_ads_community.ingest_leads_ads_community),
        ("website_form_responses", website_form_responses.ingest_website_form_responses),
        ("leads_course_strategi_ads", leads_course_strategi_ads.ingest_leads_course_strategi_ads),
        ("branding_level_up", branding_level_up.ingest_branding_level_up),
    ]

    for source_name, ingest_func in sources:
        logger.info(f"Running ingestion: {source_name}")
        try:
            ingest_func()
            logger.info(f"Successfully completed: {source_name}")
        except Exception as e:
            logger.error(f"Error running {source_name}: {str(e)}")
            raise


def run_dbt():
    """
    Run dbt to transform the ingested data.
    """
    logger.info("Starting dbt transformation")

    dbt_dir = project_root / "dbt"

    try:
        # Run dbt deps to install dependencies
        logger.info("Running dbt deps")
        subprocess.run(
            ["dbt", "deps"],
            cwd=dbt_dir,
            check=True,
            env={**os.environ, "DBT_PROFILES_DIR": str(dbt_dir)}
        )

        # Run dbt build (runs models, tests, and snapshots)
        logger.info("Running dbt build")
        result = subprocess.run(
            ["dbt", "build"],
            cwd=dbt_dir,
            check=True,
            env={**os.environ, "DBT_PROFILES_DIR": str(dbt_dir)}
        )

        logger.info("dbt transformation completed successfully")
        return result.returncode == 0

    except subprocess.CalledProcessError as e:
        logger.error(f"dbt command failed: {str(e)}")
        raise
    except FileNotFoundError:
        logger.error("dbt command not found. Is dbt installed?")
        raise


def main():
    """
    Main orchestration function.
    """
    start_time = datetime.now()
    logger.info(f"Pipeline started at {start_time}")

    try:
        # Step 1: Run ingestion scripts
        run_ingestion_scripts()

        # Step 2: Run dbt transformations
        run_dbt()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Pipeline completed successfully in {duration:.2f} seconds")

    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
