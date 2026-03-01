"""
Snowflake ETL Pipeline - Data Warehouse Loading Module

A professional ETL framework for loading data into Snowflake data warehouse
using a Snowflake schema design pattern.
"""

__version__ = "1.0.0"
__author__ = "Data Engineering Team"

from snowflake_etl.core.config import Settings
from snowflake_etl.core.database import SnowflakeConnection
from snowflake_etl.core.logger import ETLLogger

__all__ = ["Settings", "SnowflakeConnection", "ETLLogger"]
