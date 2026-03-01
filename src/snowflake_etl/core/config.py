"""
Configuration management for the ETL pipeline.

Supports loading configuration from JSON files and environment variables.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

from snowflake_etl.core.exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """Snowflake database connection configuration."""

    account: str
    user: str
    password: str
    warehouse: str
    database: str
    role: str = "ACCOUNTADMIN"
    schema: str = "PUBLIC"

    def validate(self) -> None:
        """Validate required database configuration fields."""
        required_fields = ["account", "user", "password", "warehouse", "database"]
        for field_name in required_fields:
            if not getattr(self, field_name):
                raise ConfigurationError(
                    f"Missing required database configuration: {field_name}",
                    config_key=field_name,
                )


@dataclass
class SchemaConfig:
    """Schema configuration for staging, temp, and target tables."""

    staging: str = "STG"
    temporary: str = "TMP"
    target: str = "TGT"


@dataclass
class LogConfig:
    """Logging configuration."""

    log_path: str = "./logs"
    log_level: str = "INFO"
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


@dataclass
class Settings:
    """
    Main settings class that holds all configuration.

    Can be initialized from a JSON config file or environment variables.
    """

    database: DatabaseConfig
    schemas: SchemaConfig = field(default_factory=SchemaConfig)
    log: LogConfig = field(default_factory=LogConfig)

    @classmethod
    def from_json(cls, config_path: str | Path) -> "Settings":
        """
        Load settings from a JSON configuration file.
        :rparam config_path: Path to the JSON config file.
        :returns: Settings instance populated from the config file.
        :raises ConfigurationError: If the file is missing, invalid, or required fields are missing
        """
        config_path = Path(config_path)

        if not config_path.exists():
            raise ConfigurationError(
                f"Configuration file not found: {config_path}",
                config_key="config_path",
            )

        try:
            with open(config_path) as f:
                config_data = json.load(f)
        except json.JSONDecodeError as e:
            raise ConfigurationError(
                f"Invalid JSON in configuration file: {e}",
                config_key="config_path",
            )

        # Build database config
        db_config = DatabaseConfig(
            account=config_data.get("account", ""),
            user=config_data.get("user", ""),
            password=config_data.get("password", ""),
            warehouse=config_data.get("warehouse", ""),
            database=config_data.get("database", ""),
            role=config_data.get("role", "ACCOUNTADMIN"),
            schema=config_data.get("schema", "PUBLIC"),
        )

        # Build schema config
        schema_config = SchemaConfig(
            staging=config_data.get("stg_schema", "STG"),
            temporary=config_data.get("tmp_schema", "TMP"),
            target=config_data.get("tgt_schema", "TGT"),
        )

        # Build log config
        log_config = LogConfig(
            log_path=config_data.get("log_path", "./logs"),
            log_level=config_data.get("log_level", "INFO"),
        )

        return cls(database=db_config, schemas=schema_config, log=log_config)

    @classmethod
    def from_environment(cls) -> "Settings":
        """
        Load settings from environment variables.
        :returns: Settings instance populated from environment variables.
        """
        db_config = DatabaseConfig(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD", ""),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", ""),
            database=os.getenv("SNOWFLAKE_DATABASE", ""),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        )

        schema_config = SchemaConfig(
            staging=os.getenv("ETL_STG_SCHEMA", "STG"),
            temporary=os.getenv("ETL_TMP_SCHEMA", "TMP"),
            target=os.getenv("ETL_TGT_SCHEMA", "TGT"),
        )

        log_config = LogConfig(
            log_path=os.getenv("ETL_LOG_PATH", "./logs"),
            log_level=os.getenv("ETL_LOG_LEVEL", "INFO"),
        )

        return cls(database=db_config, schemas=schema_config, log=log_config)

    def validate(self) -> None:
        """Validate all configuration settings."""
        self.database.validate()
