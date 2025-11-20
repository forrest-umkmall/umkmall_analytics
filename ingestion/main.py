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

    # Import dbt CLI
    from dbt.cli.main import dbtRunner, dbtRunnerResult

    # Initialize dbt runner
    dbt = dbtRunner()

    # Set environment for dbt
    os.environ["DBT_PROFILES_DIR"] = str(dbt_dir)

    # Change to dbt directory
    original_dir = os.getcwd()
    os.chdir(dbt_dir)

    try:
        # Run dbt deps to install dependencies
        logger.info("Running dbt deps")
        result: dbtRunnerResult = dbt.invoke(["deps"])
        if not result.success:
            raise Exception(f"dbt deps failed: {result.exception}")

        # Run dbt build (runs models, tests, and snapshots)
        # Use DBT_TARGET env var if set, otherwise use default from profile
        target = os.environ.get("DBT_TARGET")
        build_args = ["build"]
        if target:
            build_args.extend(["--target", target])
            logger.info(f"Running dbt build with target: {target}")
        else:
            logger.info("Running dbt build")

        result: dbtRunnerResult = dbt.invoke(build_args)
        if not result.success:
            raise Exception(f"dbt build failed: {result.exception}")

        logger.info("dbt transformation completed successfully")
        return True

    except Exception as e:
        logger.error(f"dbt command failed: {str(e)}")
        raise
    finally:
        os.chdir(original_dir)


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
