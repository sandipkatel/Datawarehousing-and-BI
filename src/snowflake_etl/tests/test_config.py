"""
Unit tests for configuration module.
"""

import json
import os
import tempfile

import pytest

from snowflake_etl.core.config import (
    DatabaseConfig,
    LogConfig,
    SchemaConfig,
    Settings,
)
from snowflake_etl.core.exceptions import ConfigurationError


class TestDatabaseConfig:
    """Tests for DatabaseConfig dataclass."""

    def test_valid_config(self):
        """Test valid database configuration."""
        config = DatabaseConfig(
            account="test-account",
            user="test-user",
            password="test-password",
            warehouse="test-warehouse",
            database="test-database",
        )
        config.validate()  # Should not raise

    def test_missing_required_field(self):
        """Test that missing required fields raise ConfigurationError."""
        config = DatabaseConfig(
            account="",
            user="test-user",
            password="test-password",
            warehouse="test-warehouse",
            database="test-database",
        )
        with pytest.raises(ConfigurationError):
            config.validate()


class TestSchemaConfig:
    """Tests for SchemaConfig dataclass."""

    def test_default_values(self):
        """Test default schema values."""
        config = SchemaConfig()
        assert config.staging == "STG"
        assert config.temporary == "TMP"
        assert config.target == "TGT"

    def test_custom_values(self):
        """Test custom schema values."""
        config = SchemaConfig(
            staging="MY_STG",
            temporary="MY_TMP",
            target="MY_TGT",
        )
        assert config.staging == "MY_STG"


class TestLogConfig:
    """Tests for LogConfig dataclass."""

    def test_default_values(self):
        """Test default log configuration."""
        config = LogConfig()
        assert config.log_path == "./logs"
        assert config.log_level == "INFO"


class TestSettings:
    """Tests for Settings class."""

    def test_from_json(self):
        """Test loading settings from JSON file."""
        config_data = {
            "account": "test-account",
            "user": "test-user",
            "password": "test-password",
            "warehouse": "test-warehouse",
            "database": "test-database",
            "role": "ACCOUNTADMIN",
            "stg_schema": "STG",
            "tmp_schema": "TMP",
            "tgt_schema": "TGT",
        }

        # Create temp config file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(config_data, f)
            temp_path = f.name

        try:
            settings = Settings.from_json(temp_path)
            assert settings.database.account == "test-account"
            assert settings.schemas.staging == "STG"
        finally:
            os.unlink(temp_path)

    def test_from_json_file_not_found(self):
        """Test that missing config file raises ConfigurationError."""
        with pytest.raises(ConfigurationError):
            Settings.from_json("/nonexistent/path/config.json")

    def test_from_environment(self, monkeypatch):
        """Test loading settings from environment variables."""
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env-account")
        monkeypatch.setenv("SNOWFLAKE_USER", "env-user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "env-password")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "env-warehouse")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "env-database")
        monkeypatch.setenv("ETL_STG_SCHEMA", "ENV_STG")

        settings = Settings.from_environment()
        assert settings.database.account == "env-account"
        assert settings.schemas.staging == "ENV_STG"
