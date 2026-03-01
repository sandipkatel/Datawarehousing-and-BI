#!/usr/bin/env python3
"""
Snowflake ETL Pipeline - Command Line Interface

Entry point for running the ETL pipeline with various options.
"""

import argparse
import sys
from pathlib import Path

from snowflake_etl.core.config import Settings
from snowflake_etl.core.exceptions import ConfigurationError, ETLException
from snowflake_etl.core.logger import ETLLogger
from snowflake_etl.orchestrator import ETLOrchestrator


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Snowflake ETL Pipeline - Data Warehouse Loading",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python -m snowflake_etl.main --config config.json

  # Run only dimension loads
  python -m snowflake_etl.main --config config.json --dimensions-only

  # Run only fact loads
  python -m snowflake_etl.main --config config.json --facts-only

  # Skip specific loaders
  python -m snowflake_etl.main --config config.json --skip DateLoader

  # Run a single loader
  python -m snowflake_etl.main --config config.json --loader CustomerLoader
        """,
    )

    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config.json",
        help="Path to configuration file (default: config.json)",
    )

    parser.add_argument(
        "--dimensions-only",
        "-d",
        action="store_true",
        help="Run only dimension loaders",
    )

    parser.add_argument(
        "--facts-only",
        "-f",
        action="store_true",
        help="Run only fact loaders",
    )

    parser.add_argument(
        "--skip",
        "-s",
        nargs="+",
        default=[],
        help="List of loader names to skip",
    )

    parser.add_argument(
        "--loader",
        "-l",
        type=str,
        help="Run a single specific loader by name",
    )

    parser.add_argument(
        "--env",
        action="store_true",
        help="Use environment variables instead of config file",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)",
    )

    parser.add_argument(
        "--log-dir",
        type=str,
        default="./logs",
        help="Directory for log files (default: ./logs)",
    )

    return parser.parse_args()


def main() -> int:
    """
    Main entry point for the ETL pipeline.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    args = parse_args()

    # Setup logging
    logger = ETLLogger.get_logger("ETLMain", log_dir=Path(args.log_dir))
    logger.info("=" * 60)
    logger.info("SNOWFLAKE ETL PIPELINE STARTING")
    logger.info("=" * 60)

    try:
        # Load configuration
        if args.env:
            logger.info("Loading configuration from environment variables")
            settings = Settings.from_environment()
        else:
            config_path = Path(args.config)
            logger.info(f"Loading configuration from: {config_path}")
            settings = Settings.from_json(config_path)

        # Validate configuration
        settings.validate()
        logger.info("Configuration validated successfully")

        # Create orchestrator
        orchestrator = ETLOrchestrator(settings, logger)

        # Execute pipeline
        if args.loader:
            # Run single loader
            logger.info(f"Running single loader: {args.loader}")
            result = orchestrator.run_single(args.loader)

            if result.status.value == "success":
                logger.info(f"Loader {args.loader} completed successfully")
                return 0
            else:
                logger.error(f"Loader {args.loader} failed: {result.error_message}")
                return 1
        else:
            # Run full or partial pipeline
            result = orchestrator.run(
                dimensions_only=args.dimensions_only,
                facts_only=args.facts_only,
                skip_loaders=args.skip,
            )

            if result.success:
                logger.info("Pipeline completed successfully")
                return 0
            else:
                logger.error("Pipeline completed with failures")
                return 1

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except ETLException as e:
        logger.error(f"ETL error: {e}")
        return 1
    except Exception as e:
        logger.critical(f"Unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
