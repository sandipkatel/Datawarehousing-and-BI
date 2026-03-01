"""
ETL Pipeline Orchestrator.

Manages the execution order and dependencies of all ETL loaders.
"""

from dataclasses import dataclass, field
from enum import Enum

from snowflake_etl.core.config import Settings
from snowflake_etl.core.database import SnowflakeConnection
from snowflake_etl.core.logger import ETLLogger
from snowflake_etl.loaders.base import BaseLoader
from snowflake_etl.loaders.dimension import (
    CategoryLoader,
    CityLoader,
    CountryLoader,
    CustomerLoader,
    DateLoader,
    ProductLoader,
    RegionLoader,
    SegmentLoader,
    StateLoader,
    SubcategoryLoader,
)
from snowflake_etl.loaders.fact import FactSalesLoader


class LoaderStatus(Enum):
    """Status of a loader execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class LoaderResult:
    """Result of a loader execution."""

    loader_name: str
    status: LoaderStatus
    error_message: str | None = None
    rows_affected: int | None = None


@dataclass
class PipelineResult:
    """Result of the entire pipeline execution."""

    success: bool
    results: list[LoaderResult] = field(default_factory=list)
    total_duration_seconds: float = 0.0

    def summary(self) -> str:
        """Generate a pipeline summary."""
        successful = sum(1 for r in self.results if r.status == LoaderStatus.SUCCESS)
        failed = sum(1 for r in self.results if r.status == LoaderStatus.FAILED)
        skipped = sum(1 for r in self.results if r.status == LoaderStatus.SKIPPED)

        lines = [
            "=" * 60,
            "PIPELINE EXECUTION SUMMARY",
            "=" * 60,
            f"Total Loaders: {len(self.results)}",
            f"Successful: {successful}",
            f"Failed: {failed}",
            f"Skipped: {skipped}",
            f"Duration: {self.total_duration_seconds:.2f} seconds",
            "=" * 60,
        ]

        if failed > 0:
            lines.append("\nFailed Loaders:")
            for r in self.results:
                if r.status == LoaderStatus.FAILED:
                    lines.append(f"  - {r.loader_name}: {r.error_message}")

        return "\n".join(lines)


class ETLOrchestrator:
    """
    Orchestrates the execution of all ETL loaders in the correct order.

    Manages dependencies between dimension and fact tables, ensuring
    parent dimensions are loaded before child dimensions and facts.
    """

    # Loaders in dependency order for Snowflake schema
    DIMENSION_LOADERS: list[type[BaseLoader]] = [
        CountryLoader,  # Level 1 - No dependencies
        RegionLoader,  # Level 1 - No dependencies
        StateLoader,  # Level 1 - No dependencies (could depend on Region)
        CategoryLoader,  # Level 1 - No dependencies
        SegmentLoader,  # Level 1 - No dependencies
        DateLoader,  # Level 1 - No dependencies (generated)
        CityLoader,  # Level 2 - Depends on State
        SubcategoryLoader,  # Level 2 - Depends on Category
        ProductLoader,  # Level 3 - Depends on Subcategory
        CustomerLoader,  # Level 2 - Depends on Segment
    ]

    FACT_LOADERS: list[type[BaseLoader]] = [
        FactSalesLoader,  # Depends on all dimensions
    ]

    def __init__(
        self,
        settings: Settings,
        logger: ETLLogger | None = None,
    ):
        """
        Initialize the orchestrator.

        Args:
            settings: Application settings.
            logger: Optional ETL logger instance.
        """
        self.settings = settings
        self.logger = logger or ETLLogger.get_logger("ETLOrchestrator")

    def run(
        self,
        dimensions_only: bool = False,
        facts_only: bool = False,
        skip_loaders: list[str] | None = None,
    ) -> PipelineResult:
        """
        Execute the ETL pipeline.

        Args:
            dimensions_only: Only run dimension loaders.
            facts_only: Only run fact loaders.
            skip_loaders: List of loader names to skip.

        Returns:
            PipelineResult with execution details.
        """
        import time

        start_time = time.time()

        skip_loaders = skip_loaders or []
        results: list[LoaderResult] = []

        self.logger.info("Starting ETL Pipeline Execution")
        self.logger.info(f"Dimensions only: {dimensions_only}")
        self.logger.info(f"Facts only: {facts_only}")
        self.logger.info(f"Skip loaders: {skip_loaders}")

        # Determine which loaders to run
        loaders_to_run: list[type[BaseLoader]] = []

        if not facts_only:
            loaders_to_run.extend(self.DIMENSION_LOADERS)

        if not dimensions_only:
            loaders_to_run.extend(self.FACT_LOADERS)

        # Execute loaders
        pipeline_success = True

        with SnowflakeConnection(self.settings, self.logger) as conn:
            for loader_class in loaders_to_run:
                loader_name = loader_class.__name__

                # Check if loader should be skipped
                if loader_name in skip_loaders:
                    self.logger.info(f"Skipping loader: {loader_name}")
                    results.append(
                        LoaderResult(
                            loader_name=loader_name,
                            status=LoaderStatus.SKIPPED,
                        )
                    )
                    continue

                # Execute the loader
                try:
                    loader = loader_class(conn, self.settings, self.logger)
                    loader.execute()

                    results.append(
                        LoaderResult(
                            loader_name=loader_name,
                            status=LoaderStatus.SUCCESS,
                        )
                    )

                except Exception as e:
                    error_msg = str(e)
                    self.logger.error(f"Loader {loader_name} failed: {error_msg}")

                    results.append(
                        LoaderResult(
                            loader_name=loader_name,
                            status=LoaderStatus.FAILED,
                            error_message=error_msg,
                        )
                    )
                    pipeline_success = False

                    # Stop pipeline on failure (can be made configurable)
                    self.logger.error("Pipeline stopped due to loader failure")
                    break

        duration = time.time() - start_time

        result = PipelineResult(
            success=pipeline_success,
            results=results,
            total_duration_seconds=duration,
        )

        self.logger.info(result.summary())
        return result

    def run_single(self, loader_name: str) -> LoaderResult:
        """
        Run a single loader by name.

        Args:
            loader_name: Name of the loader class to run.

        Returns:
            LoaderResult with execution details.
        """
        # Find the loader class
        all_loaders = self.DIMENSION_LOADERS + self.FACT_LOADERS
        loader_class = None

        for lc in all_loaders:
            if lc.__name__ == loader_name:
                loader_class = lc
                break

        if loader_class is None:
            return LoaderResult(
                loader_name=loader_name,
                status=LoaderStatus.FAILED,
                error_message=f"Loader not found: {loader_name}",
            )

        # Execute
        with SnowflakeConnection(self.settings, self.logger) as conn:
            try:
                loader = loader_class(conn, self.settings, self.logger)
                loader.execute()
                return LoaderResult(
                    loader_name=loader_name,
                    status=LoaderStatus.SUCCESS,
                )
            except Exception as e:
                return LoaderResult(
                    loader_name=loader_name,
                    status=LoaderStatus.FAILED,
                    error_message=str(e),
                )
