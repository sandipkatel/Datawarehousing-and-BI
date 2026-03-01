"""
Logging module for the ETL pipeline.

Provides structured logging with file and console outputs.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

from snowflake_etl.core.config import LogConfig


class ETLLogger:
    """
    ETL-specific logger with file and console handlers.

    Provides a consistent logging interface across all ETL components.
    """

    _instances: dict[str, "ETLLogger"] = {}

    def __init__(
        self,
        name: str,
        config: LogConfig | None = None,
        log_dir: Path | None = None,
    ):
        """
        Initialize the ETL logger.

        Args:
            name: Logger name (typically the loader/script name).
            config: Optional LogConfig instance.
            log_dir: Optional directory for log files.
        """
        self.name = name
        self.config = config or LogConfig()

        # Setup log directory
        if log_dir:
            self.log_dir = Path(log_dir)
        else:
            self.log_dir = Path(self.config.log_path)

        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Create logger
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, self.config.log_level.upper(), logging.INFO))

        # Prevent duplicate handlers
        if not self.logger.handlers:
            self._setup_handlers()

    def _setup_handlers(self) -> None:
        """Configure file and console handlers."""
        formatter = logging.Formatter(self.config.log_format)

        # File handler with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"{self.name}_{timestamp}.log"
        file_handler = logging.FileHandler(log_file, mode="w")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    @classmethod
    def get_logger(
        cls,
        name: str,
        config: LogConfig | None = None,
        log_dir: Path | None = None,
    ) -> "ETLLogger":
        """
        Get or create a logger instance (singleton per name).

        Args:
            name: Logger name.
            config: Optional LogConfig instance.
            log_dir: Optional log directory path.

        Returns:
            ETLLogger instance.
        """
        if name not in cls._instances:
            cls._instances[name] = cls(name, config, log_dir)
        return cls._instances[name]

    def info(self, message: str) -> None:
        """Log an info message."""
        self.logger.info(message)

    def debug(self, message: str) -> None:
        """Log a debug message."""
        self.logger.debug(message)

    def warning(self, message: str) -> None:
        """Log a warning message."""
        self.logger.warning(message)

    def error(self, message: str, exc_info: bool = False) -> None:
        """Log an error message."""
        self.logger.error(message, exc_info=exc_info)

    def critical(self, message: str, exc_info: bool = True) -> None:
        """Log a critical message."""
        self.logger.critical(message, exc_info=exc_info)

    def log_query(self, query: str, truncate: int = 500) -> None:
        """
        Log a SQL query with optional truncation.

        Args:
            query: SQL query string.
            truncate: Maximum length before truncation.
        """
        if len(query) > truncate:
            display_query = query[:truncate] + "..."
        else:
            display_query = query
        self.debug(f"Executing query: {display_query}")

    def log_query_result(self, row_count: int) -> None:
        """Log query result row count."""
        self.debug(f"Query returned {row_count} rows")

    def log_loader_start(self, loader_name: str) -> None:
        """Log loader start."""
        self.info(f"{'=' * 50}")
        self.info(f"Starting loader: {loader_name}")
        self.info(f"{'=' * 50}")

    def log_loader_complete(self, loader_name: str, success: bool = True) -> None:
        """Log loader completion."""
        status = "completed successfully" if success else "failed"
        self.info(f"Loader {loader_name} {status}")
        self.info(f"{'=' * 50}")
