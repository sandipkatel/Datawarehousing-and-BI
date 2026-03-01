"""
Unit tests for exception classes.
"""

from snowflake_etl.core.exceptions import (
    ConfigurationError,
    DatabaseConnectionError,
    ETLException,
    LoaderError,
    QueryExecutionError,
)


class TestETLException:
    """Tests for base ETLException."""

    def test_message_only(self):
        """Test exception with message only."""
        exc = ETLException("Test error")
        assert str(exc) == "Test error"
        assert exc.message == "Test error"
        assert exc.details is None

    def test_message_with_details(self):
        """Test exception with message and details."""
        exc = ETLException("Test error", details="Additional info")
        assert str(exc) == "Test error | Details: Additional info"


class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_with_config_key(self):
        """Test configuration error with config key."""
        exc = ConfigurationError("Missing config", config_key="database")
        assert "database" in str(exc)

    def test_without_config_key(self):
        """Test configuration error without config key."""
        exc = ConfigurationError("Missing config")
        assert str(exc) == "Missing config"


class TestDatabaseConnectionError:
    """Tests for DatabaseConnectionError."""

    def test_with_account(self):
        """Test connection error with account info."""
        exc = DatabaseConnectionError("Connection failed", account="test-account")
        assert "test-account" in str(exc)


class TestQueryExecutionError:
    """Tests for QueryExecutionError."""

    def test_query_truncation(self):
        """Test that long queries are truncated."""
        long_query = "SELECT " + "x, " * 100 + "y FROM table"
        exc = QueryExecutionError("Query failed", query=long_query)
        assert "..." in str(exc)
        assert len(exc.details) < len(long_query) + 50


class TestLoaderError:
    """Tests for LoaderError."""

    def test_with_loader_name(self):
        """Test loader error with loader name."""
        exc = LoaderError("Load failed", loader_name="CustomerLoader")
        assert "CustomerLoader" in str(exc)
