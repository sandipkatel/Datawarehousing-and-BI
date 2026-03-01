"""
Base loader classes for ETL operations.

Provides abstract base classes for dimension and fact table loaders
implementing the SCD Type 2 pattern.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

from snowflake_etl.core.config import Settings
from snowflake_etl.core.database import SnowflakeConnection
from snowflake_etl.core.exceptions import LoaderError
from snowflake_etl.core.logger import ETLLogger


@dataclass
class LoaderConfig:
    """Configuration for a loader."""

    staging_view: str
    temp_table: str
    target_table: str
    staging_columns: list[str]
    temp_columns: list[str]
    target_columns: list[str]
    business_keys: list[str]
    change_detection_columns: list[str]


class BaseLoader(ABC):
    """
    Abstract base class for all ETL loaders.

    Provides common functionality for truncating temp tables,
    loading from staging, and managing SCD Type 2 dimensions.
    """

    def __init__(
        self,
        connection: SnowflakeConnection,
        settings: Settings,
        logger: ETLLogger | None = None,
    ):
        """
        Initialize the loader.

        Args:
            connection: Active Snowflake connection.
            settings: Application settings.
            logger: Optional ETL logger instance.
        """
        self.conn = connection
        self.settings = settings
        self.stg_schema = settings.schemas.staging
        self.tmp_schema = settings.schemas.temporary
        self.tgt_schema = settings.schemas.target
        self.logger = logger or ETLLogger.get_logger(self.__class__.__name__)

    @property
    @abstractmethod
    def name(self) -> str:
        """Loader name for identification."""
        pass

    @property
    @abstractmethod
    def config(self) -> LoaderConfig:
        """Loader-specific configuration."""
        pass

    def execute(self) -> bool:
        """
        Execute the complete load process.

        Returns:
            True if successful, False otherwise.
        """
        self.logger.log_loader_start(self.name)

        try:
            self._truncate_temp_table()
            self._load_to_temp()
            self._process_changes()
            self.logger.log_loader_complete(self.name, success=True)
            return True
        except Exception as e:
            self.logger.error(f"Loader {self.name} failed: {e}", exc_info=True)
            self.logger.log_loader_complete(self.name, success=False)
            raise LoaderError(str(e), loader_name=self.name)

    def _truncate_temp_table(self) -> None:
        """Truncate the temporary staging table."""
        self.conn.truncate_table(self.tmp_schema, self.config.temp_table)

    @abstractmethod
    def _load_to_temp(self) -> None:
        """Load data from staging to temporary table."""
        pass

    @abstractmethod
    def _process_changes(self) -> None:
        """Process changes (expire old records, insert new)."""
        pass


class BaseDimensionLoader(BaseLoader):
    """
    Base class for dimension table loaders.

    Implements SCD Type 2 pattern with:
    - IS_CURRENT flag
    - EFFECTIVE_FROM/EFFECTIVE_TO timestamps
    """

    # Default SCD columns added to all dimensions
    SCD_COLUMNS = ["IS_CURRENT", "EFFECTIVE_FROM", "EFFECTIVE_TO"]
    END_DATE = "TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999')"

    def _build_expire_query(
        self,
        src_cte: str,
        join_conditions: list[str],
        change_conditions: list[str],
    ) -> str:
        """
        Build the SCD Type 2 expire query.

        Args:
            src_cte: Source CTE/subquery for comparison.
            join_conditions: List of join conditions on business keys.
            change_conditions: List of conditions detecting attribute changes.

        Returns:
            SQL UPDATE query string.
        """
        join_clause = " AND ".join(join_conditions)
        change_clause = " OR ".join(change_conditions)

        return f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM (
                {src_cte}
            ) AS SRC
            WHERE {join_clause}
              AND TGT.IS_CURRENT = TRUE
              AND ({change_clause})
        """

    def _build_insert_query(
        self,
        src_cte: str,
        target_columns: list[str],
        source_columns: list[str],
        join_conditions: list[str],
        null_check_column: str,
    ) -> str:
        """
        Build the SCD Type 2 insert query for new/changed records.

        Args:
            src_cte: Source CTE/subquery.
            target_columns: List of target table columns.
            source_columns: List of corresponding source columns.
            join_conditions: Join conditions for duplicate check.
            null_check_column: Column to check for NULL (indicating new record).

        Returns:
            SQL INSERT query string.
        """
        tgt_cols = ", ".join(target_columns + self.SCD_COLUMNS)
        src_cols = ", ".join(source_columns)
        join_clause = " AND ".join(join_conditions)

        return f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            ({tgt_cols})
            SELECT
                {src_cols},
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM (
                {src_cte}
            ) SRC
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON {join_clause}
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.{null_check_column} IS NULL
        """


class BaseFactLoader(BaseLoader):
    """
    Base class for fact table loaders.

    Implements MERGE pattern for fact tables with:
    - Surrogate key lookup from dimensions
    - Upsert (update existing, insert new) logic
    """

    def _build_merge_query(
        self,
        src_cte: str,
        match_columns: list[str],
        update_columns: list[str],
        insert_columns: list[str],
    ) -> str:
        """
        Build a MERGE query for fact table loading.

        Args:
            src_cte: Source CTE with dimension key lookups.
            match_columns: Columns used to match existing records.
            update_columns: Columns to update on match.
            insert_columns: Columns for new record insertion.

        Returns:
            SQL MERGE query string.
        """
        match_clause = " AND ".join([f"TGT.{col} = SRC.{col}" for col in match_columns])
        update_clause = ",\n".join([f"TGT.{col} = SRC.{col}" for col in update_columns])
        insert_cols = ", ".join([f"TGT.{col}" for col in insert_columns])
        insert_vals = ", ".join([f"SRC.{col}" for col in insert_columns])

        return f"""
            MERGE INTO {self.tgt_schema}.{self.config.target_table} AS TGT
            USING (
                {src_cte}
            ) AS SRC
                ON {match_clause}
            WHEN MATCHED THEN
                UPDATE SET
                    {update_clause}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols})
                VALUES ({insert_vals})
        """
