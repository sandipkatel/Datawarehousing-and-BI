"""Custom exceptions for the ETL pipeline."""


class ETLException(Exception):
    """Base exception for all ETL-related errors."""

    def __init__(self, message: str, details: str | None = None):
        self.message = message
        self.details = details
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.details:
            return f"{self.message} | Details: {self.details}"
        return self.message


class ConfigurationError(ETLException):
    """Raised when there is a configuration error."""

    def __init__(self, message: str, config_key: str | None = None):
        details = f"Config key: {config_key}" if config_key else None
        super().__init__(message, details)


class DatabaseConnectionError(ETLException):
    """Raised when database connection fails."""

    def __init__(self, message: str, account: str | None = None):
        details = f"Account: {account}" if account else None
        super().__init__(message, details)


class QueryExecutionError(ETLException):
    """Raised when a query execution fails."""

    def __init__(self, message: str, query: str | None = None):
        # Truncate query for readability
        if query and len(query) > 200:
            query = query[:200] + "..."
        details = f"Query: {query}" if query else None
        super().__init__(message, details)


class LoaderError(ETLException):
    """Raised when a loader operation fails."""

    def __init__(self, message: str, loader_name: str | None = None):
        details = f"Loader: {loader_name}" if loader_name else None
        super().__init__(message, details)
