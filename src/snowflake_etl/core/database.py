"""
Snowflake database connection management.

Provides a thread-safe connection handler with context manager support.
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import snowflake.connector
from snowflake.connector import SnowflakeConnection as SFConnection
from snowflake.connector.cursor import SnowflakeCursor

from snowflake_etl.core.config import Settings
from snowflake_etl.core.exceptions import DatabaseConnectionError, QueryExecutionError
from snowflake_etl.core.logger import ETLLogger


class SnowflakeConnection:
    """
    Snowflake database connection handler.

    Provides connection management with context manager support,
    query execution, and proper resource cleanup.
    """

    def __init__(
        self,
        settings: Settings,
        logger: ETLLogger | None = None,
    ):
        """
        Initialize the Snowflake connection handler.

        Args:
            settings: Application settings with database configuration.
            logger: Optional ETL logger instance.
        """
        self.settings = settings
        self.db_config = settings.database
        self.logger = logger or ETLLogger.get_logger("SnowflakeConnection")
        self._connection: SFConnection | None = None
        self._cursor: SnowflakeCursor | None = None

    def connect(self) -> None:
        """
        Establish connection to Snowflake.

        Raises:
            DatabaseConnectionError: If connection fails.
        """
        if self._connection is not None:
            self.logger.debug("Connection already established")
            return

        try:
            self.logger.info(f"Connecting to Snowflake account: {self.db_config.account}")
            self._connection = snowflake.connector.connect(
                user=self.db_config.user,
                password=self.db_config.password,
                account=self.db_config.account,
                database=self.db_config.database,
                warehouse=self.db_config.warehouse,
                role=self.db_config.role,
                client_telemetry_enabled=False,
            )
            self._cursor = self._connection.cursor()
            self.logger.info("Successfully connected to Snowflake")
        except Exception as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise DatabaseConnectionError(
                f"Failed to connect to Snowflake: {e}",
                account=self.db_config.account,
            )

    def disconnect(self) -> None:
        """Close the Snowflake connection and cursor."""
        if self._cursor:
            try:
                self._cursor.close()
                self.logger.debug("Cursor closed")
            except Exception as e:
                self.logger.warning(f"Error closing cursor: {e}")
            finally:
                self._cursor = None

        if self._connection:
            try:
                self._connection.close()
                self.logger.info("Disconnected from Snowflake")
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None

    def __enter__(self) -> "SnowflakeConnection":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit with cleanup."""
        self.disconnect()

    @property
    def cursor(self) -> SnowflakeCursor:
        """Get the active cursor, connecting if necessary."""
        if self._cursor is None:
            self.connect()
        return self._cursor

    def execute(
        self,
        query: str,
        params: tuple | None = None,
        fetch: bool = True,
    ) -> list[tuple[Any, ...]] | None:
        """
        Execute a SQL query.

        Args:
            query: SQL query string.
            params: Optional query parameters.
            fetch: Whether to fetch and return results.

        Returns:
            List of result tuples if fetch=True, None otherwise.

        Raises:
            QueryExecutionError: If query execution fails.
        """
        self.logger.log_query(query)

        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if fetch:
                results = self.cursor.fetchall()
                self.logger.log_query_result(len(results))
                return results
            return None

        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            raise QueryExecutionError(
                f"Query execution failed: {e}",
                query=query,
            )

    def execute_many(
        self,
        query: str,
        params_list: list[tuple],
    ) -> None:
        """
        Execute a query with multiple parameter sets.

        Args:
            query: SQL query string with placeholders.
            params_list: List of parameter tuples.

        Raises:
            QueryExecutionError: If execution fails.
        """
        self.logger.log_query(query)
        self.logger.debug(f"Executing with {len(params_list)} parameter sets")

        try:
            self.cursor.executemany(query, params_list)
            self.logger.debug(f"Successfully executed batch of {len(params_list)} rows")
        except Exception as e:
            self.logger.error(f"Batch execution failed: {e}")
            raise QueryExecutionError(
                f"Batch execution failed: {e}",
                query=query,
            )

    def truncate_table(self, schema: str, table: str) -> None:
        """
        Truncate a table.

        Args:
            schema: Schema name.
            table: Table name.
        """
        query = f"TRUNCATE TABLE {schema}.{table}"
        self.execute(query, fetch=False)
        self.logger.info(f"Truncated table {schema}.{table}")

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """
        Context manager for database transactions.

        Yields:
            None

        Example:
            with connection.transaction():
                connection.execute(query1)
                connection.execute(query2)
        """
        try:
            self.execute("BEGIN TRANSACTION", fetch=False)
            yield
            self.execute("COMMIT", fetch=False)
            self.logger.debug("Transaction committed")
        except Exception as e:
            self.execute("ROLLBACK", fetch=False)
            self.logger.error(f"Transaction rolled back: {e}")
            raise

    def get_row_count(self, schema: str, table: str) -> int:
        """
        Get the row count of a table.

        Args:
            schema: Schema name.
            table: Table name.

        Returns:
            Number of rows in the table.
        """
        query = f"SELECT COUNT(*) FROM {schema}.{table}"
        result = self.execute(query)
        return result[0][0] if result else 0
