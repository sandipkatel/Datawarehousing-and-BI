"""ETL Loader modules for dimension and fact tables."""

from snowflake_etl.loaders.base import BaseDimensionLoader, BaseFactLoader
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

__all__ = [
    "BaseDimensionLoader",
    "BaseFactLoader",
    "CountryLoader",
    "RegionLoader",
    "StateLoader",
    "CityLoader",
    "CategoryLoader",
    "SubcategoryLoader",
    "ProductLoader",
    "SegmentLoader",
    "CustomerLoader",
    "DateLoader",
    "FactSalesLoader",
]
