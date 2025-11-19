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

    sources_dir = Path(__file__).parent / "sources"

    if not sources_dir.exists():
        logger.warning(f"Sources directory not found: {sources_dir}")
        return

    # Get all Python files in sources directory
    source_files = sorted(sources_dir.glob("*.py"))

    if not source_files:
        logger.warning("No ingestion scripts found in sources directory")
        return

    for source_file in source_files:
        if source_file.name.startswith("_"):
            logger.info(f"Skipping {source_file.name} (starts with underscore)")
            continue

        logger.info(f"Running ingestion script: {source_file.name}")
        try:
            # Import and run the script
            module_name = source_file.stem
            exec(open(source_file).read(), {"__name__": "__main__"})
            logger.info(f"Successfully completed: {source_file.name}")
        except Exception as e:
            logger.error(f"Error running {source_file.name}: {str(e)}")
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
