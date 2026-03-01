"""
Dimension table loaders for the Snowflake schema.

Implements SCD Type 2 loading for all dimension tables in the data warehouse.
"""

from snowflake_etl.loaders.base import BaseDimensionLoader, LoaderConfig


class CountryLoader(BaseDimensionLoader):
    """Loader for TGT_D_COUNTRY dimension table."""

    @property
    def name(self) -> str:
        return "COUNTRY_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_COUNTRY",
            temp_table="TMP_D_COUNTRY",
            target_table="TGT_D_COUNTRY",
            staging_columns=["COUNTRY"],
            temp_columns=["COUNTRY_NAME"],
            target_columns=["COUNTRY_NAME"],
            business_keys=["COUNTRY_NAME"],
            change_detection_columns=["COUNTRY_NAME"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct countries from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table} (COUNTRY_NAME)
            SELECT DISTINCT COUNTRY
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed records and insert new countries."""
        # For simple dimension, expire is typically not triggered
        # as COUNTRY_NAME is both key and attribute
        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            WHERE TGT.COUNTRY_NAME = TMP.COUNTRY_NAME
              AND TGT.IS_CURRENT = TRUE
              AND TGT.COUNTRY_NAME <> TMP.COUNTRY_NAME
        """
        self.conn.execute(expire_query, fetch=False)

        # Insert new records
        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (COUNTRY_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                TMP.COUNTRY_NAME,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.COUNTRY_NAME = TMP.COUNTRY_NAME
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.COUNTRY_NAME IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class RegionLoader(BaseDimensionLoader):
    """Loader for TGT_D_REGION dimension table."""

    @property
    def name(self) -> str:
        return "REGION_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_LOCATION",
            temp_table="TMP_D_REGION",
            target_table="TGT_D_REGION",
            staging_columns=["REGION"],
            temp_columns=["REGION_NAME"],
            target_columns=["REGION_NAME"],
            business_keys=["REGION_NAME"],
            change_detection_columns=["REGION_NAME"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct regions from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table} (REGION_NAME)
            SELECT DISTINCT REGION
            FROM {self.stg_schema}.{self.config.staging_view}
            WHERE REGION IS NOT NULL
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed records and insert new regions."""
        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            WHERE TGT.REGION_NAME = TMP.REGION_NAME
              AND TGT.IS_CURRENT = TRUE
              AND TGT.REGION_NAME <> TMP.REGION_NAME
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (REGION_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                TMP.REGION_NAME,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.REGION_NAME = TMP.REGION_NAME
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.REGION_NAME IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class StateLoader(BaseDimensionLoader):
    """Loader for TGT_D_STATE dimension table."""

    @property
    def name(self) -> str:
        return "STATE_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_LOCATION",
            temp_table="TMP_D_STATE",
            target_table="TGT_D_STATE",
            staging_columns=["STATE"],
            temp_columns=["STATE_NAME"],
            target_columns=["STATE_NAME"],
            business_keys=["STATE_NAME"],
            change_detection_columns=["STATE_NAME"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct states from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table} (STATE_NAME)
            SELECT DISTINCT STATE
            FROM {self.stg_schema}.{self.config.staging_view}
            WHERE STATE IS NOT NULL
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed records and insert new states."""
        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            WHERE TGT.STATE_NAME = TMP.STATE_NAME
              AND TGT.IS_CURRENT = TRUE
              AND TGT.STATE_NAME <> TMP.STATE_NAME
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (STATE_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                TMP.STATE_NAME,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.STATE_NAME = TMP.STATE_NAME
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.STATE_NAME IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class CityLoader(BaseDimensionLoader):
    """Loader for TGT_D_CITY dimension table with STATE_KEY lookup."""

    @property
    def name(self) -> str:
        return "CITY_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_CITY",
            temp_table="TMP_D_CITY",
            target_table="TGT_D_CITY",
            staging_columns=["CITY", "POSTAL_CODE", "STATE"],
            temp_columns=["CITY_NAME", "POSTAL_CODE", "STATE_NAME"],
            target_columns=["CITY_NAME", "POSTAL_CODE", "STATE_KEY"],
            business_keys=["CITY_NAME", "POSTAL_CODE"],
            change_detection_columns=["STATE_KEY"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct cities from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table}
            (CITY_NAME, POSTAL_CODE, STATE_NAME)
            SELECT DISTINCT CITY, POSTAL_CODE, STATE
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed cities and insert new ones with STATE_KEY lookup."""
        # Source CTE with state key lookup
        src_cte = f"""
            SELECT
                TMP.CITY_NAME,
                TMP.POSTAL_CODE,
                S.STATE_KEY
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            JOIN {self.tgt_schema}.TGT_D_STATE S
                ON TMP.STATE_NAME = S.STATE_NAME
               AND S.IS_CURRENT = TRUE
        """

        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM ({src_cte}) AS SRC
            WHERE TGT.CITY_NAME = SRC.CITY_NAME
              AND TGT.POSTAL_CODE = SRC.POSTAL_CODE
              AND TGT.IS_CURRENT = TRUE
              AND TGT.STATE_KEY <> SRC.STATE_KEY
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (CITY_NAME, POSTAL_CODE, STATE_KEY, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                SRC.CITY_NAME,
                SRC.POSTAL_CODE,
                SRC.STATE_KEY,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM ({src_cte}) SRC
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.CITY_NAME = SRC.CITY_NAME
               AND CUR.POSTAL_CODE = SRC.POSTAL_CODE
               AND CUR.STATE_KEY = SRC.STATE_KEY
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.CITY_KEY IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class CategoryLoader(BaseDimensionLoader):
    """Loader for TGT_D_CATEGORY dimension table."""

    @property
    def name(self) -> str:
        return "CATEGORY_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_CATEGORY",
            temp_table="TMP_D_CATEGORY",
            target_table="TGT_D_CATEGORY",
            staging_columns=["CATEGORY"],
            temp_columns=["CATEGORY_NAME"],
            target_columns=["CATEGORY_NAME"],
            business_keys=["CATEGORY_NAME"],
            change_detection_columns=["CATEGORY_NAME"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct categories from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table} (CATEGORY_NAME)
            SELECT DISTINCT CATEGORY
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed records and insert new categories."""
        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            WHERE TGT.CATEGORY_NAME = TMP.CATEGORY_NAME
              AND TGT.IS_CURRENT = TRUE
              AND TGT.CATEGORY_NAME <> TMP.CATEGORY_NAME
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (CATEGORY_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                TMP.CATEGORY_NAME,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.CATEGORY_NAME = TMP.CATEGORY_NAME
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.CATEGORY_NAME IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class SubcategoryLoader(BaseDimensionLoader):
    """Loader for TGT_D_SUBCATEGORY dimension table with CATEGORY_KEY lookup."""

    @property
    def name(self) -> str:
        return "SUBCATEGORY_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_SUBCATEGORY",
            temp_table="TMP_D_SUBCATEGORY",
            target_table="TGT_D_SUBCATEGORY",
            staging_columns=["SUB_CATEGORY", "CATEGORY"],
            temp_columns=["SUBCATEGORY_NAME", "CATEGORY_NAME"],
            target_columns=["SUBCATEGORY_NAME", "CATEGORY_KEY"],
            business_keys=["SUBCATEGORY_NAME"],
            change_detection_columns=["CATEGORY_KEY"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct subcategories from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table}
            (SUBCATEGORY_NAME, CATEGORY_NAME)
            SELECT DISTINCT SUB_CATEGORY, CATEGORY
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed subcategories and insert new with CATEGORY_KEY lookup."""
        src_cte = f"""
            SELECT
                TMP.SUBCATEGORY_NAME,
                C.CATEGORY_KEY
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            JOIN {self.tgt_schema}.TGT_D_CATEGORY C
                ON TMP.CATEGORY_NAME = C.CATEGORY_NAME
               AND C.IS_CURRENT = TRUE
        """

        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM ({src_cte}) AS SRC
            WHERE TGT.SUBCATEGORY_NAME = SRC.SUBCATEGORY_NAME
              AND TGT.IS_CURRENT = TRUE
              AND TGT.CATEGORY_KEY <> SRC.CATEGORY_KEY
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (SUBCATEGORY_NAME, CATEGORY_KEY, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                SRC.SUBCATEGORY_NAME,
                SRC.CATEGORY_KEY,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM ({src_cte}) SRC
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.SUBCATEGORY_NAME = SRC.SUBCATEGORY_NAME
               AND CUR.CATEGORY_KEY = SRC.CATEGORY_KEY
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.SUBCATEGORY_KEY IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class ProductLoader(BaseDimensionLoader):
    """Loader for TGT_D_PRODUCT dimension table with SUBCATEGORY_KEY lookup."""

    @property
    def name(self) -> str:
        return "PRODUCT_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_PRODUCT",
            temp_table="TMP_D_PRODUCT_SF",
            target_table="TGT_D_PRODUCT_SF",
            staging_columns=["PRODUCT_ID", "PRODUCT_NAME", "SUB_CATEGORY"],
            temp_columns=["PRODUCT_ID", "PRODUCT_NAME", "SUBCATEGORY_NAME"],
            target_columns=["PRODUCT_ID", "PRODUCT_NAME", "SUBCATEGORY_KEY"],
            business_keys=["PRODUCT_ID"],
            change_detection_columns=["PRODUCT_NAME", "SUBCATEGORY_KEY"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct products from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table}
            (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_NAME)
            SELECT DISTINCT PRODUCT_ID, PRODUCT_NAME, SUB_CATEGORY
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed products and insert new with SUBCATEGORY_KEY lookup."""
        src_cte = f"""
            SELECT
                TMP.PRODUCT_ID,
                TMP.PRODUCT_NAME,
                SC.SUBCATEGORY_KEY
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            JOIN {self.tgt_schema}.TGT_D_SUBCATEGORY SC
                ON TMP.SUBCATEGORY_NAME = SC.SUBCATEGORY_NAME
        """

        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM ({src_cte}) AS SRC
            WHERE TGT.PRODUCT_ID = SRC.PRODUCT_ID
              AND TGT.IS_CURRENT = TRUE
              AND (TGT.PRODUCT_NAME <> SRC.PRODUCT_NAME
                   OR TGT.SUBCATEGORY_KEY <> SRC.SUBCATEGORY_KEY)
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (PRODUCT_ID, PRODUCT_NAME, SUBCATEGORY_KEY, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                SRC.PRODUCT_ID,
                SRC.PRODUCT_NAME,
                SRC.SUBCATEGORY_KEY,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM ({src_cte}) SRC
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.PRODUCT_ID = SRC.PRODUCT_ID
               AND CUR.IS_CURRENT = TRUE
               AND CUR.PRODUCT_NAME = SRC.PRODUCT_NAME
               AND CUR.SUBCATEGORY_KEY = SRC.SUBCATEGORY_KEY
            WHERE CUR.PRODUCT_ID IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class SegmentLoader(BaseDimensionLoader):
    """Loader for TGT_D_SEGMENT dimension table."""

    @property
    def name(self) -> str:
        return "SEGMENT_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_SEGMENT",
            temp_table="TMP_D_SEGMENT",
            target_table="TGT_D_SEGMENT",
            staging_columns=["SEGMENT"],
            temp_columns=["SEGMENT_NAME"],
            target_columns=["SEGMENT_NAME"],
            business_keys=["SEGMENT_NAME"],
            change_detection_columns=["SEGMENT_NAME"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct segments from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table} (SEGMENT_NAME)
            SELECT DISTINCT SEGMENT
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed records and insert new segments."""
        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            WHERE TGT.SEGMENT_NAME = TMP.SEGMENT_NAME
              AND TGT.IS_CURRENT = TRUE
              AND TGT.SEGMENT_NAME <> TMP.SEGMENT_NAME
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (SEGMENT_NAME, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                TMP.SEGMENT_NAME,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.SEGMENT_NAME = TMP.SEGMENT_NAME
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.SEGMENT_NAME IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class CustomerLoader(BaseDimensionLoader):
    """Loader for TGT_D_CUSTOMER dimension table with SEGMENT_KEY lookup."""

    @property
    def name(self) -> str:
        return "CUSTOMER_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_D_CUSTOMER",
            temp_table="TMP_D_CUSTOMER_SF",
            target_table="TGT_D_CUSTOMER_SF",
            staging_columns=["CUSTOMER_ID", "CUSTOMER_NAME", "SEGMENT"],
            temp_columns=["CUSTOMER_ID", "CUSTOMER_NAME", "SEGMENT_NAME"],
            target_columns=["CUSTOMER_ID", "CUSTOMER_NAME", "SEGMENT_KEY"],
            business_keys=["CUSTOMER_ID"],
            change_detection_columns=["CUSTOMER_NAME", "SEGMENT_KEY"],
        )

    def _load_to_temp(self) -> None:
        """Load distinct customers from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table}
            (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_NAME)
            SELECT DISTINCT CUSTOMER_ID, CUSTOMER_NAME, SEGMENT
            FROM {self.stg_schema}.{self.config.staging_view}
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Expire changed customers and insert new with SEGMENT_KEY lookup."""
        src_cte = f"""
            SELECT
                TMP.CUSTOMER_ID,
                TMP.CUSTOMER_NAME,
                S.SEGMENT_KEY
            FROM {self.tmp_schema}.{self.config.temp_table} TMP
            JOIN {self.tgt_schema}.TGT_D_SEGMENT S
                ON TMP.SEGMENT_NAME = S.SEGMENT_NAME
               AND S.IS_CURRENT = TRUE
        """

        expire_query = f"""
            UPDATE {self.tgt_schema}.{self.config.target_table} AS TGT
            SET
                IS_CURRENT = FALSE,
                EFFECTIVE_TO = CURRENT_TIMESTAMP()
            FROM ({src_cte}) AS SRC
            WHERE TGT.CUSTOMER_ID = SRC.CUSTOMER_ID
              AND TGT.IS_CURRENT = TRUE
              AND (TGT.CUSTOMER_NAME <> SRC.CUSTOMER_NAME
                   OR TGT.SEGMENT_KEY <> SRC.SEGMENT_KEY)
        """
        self.conn.execute(expire_query, fetch=False)

        insert_query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            (CUSTOMER_ID, CUSTOMER_NAME, SEGMENT_KEY, IS_CURRENT, EFFECTIVE_FROM, EFFECTIVE_TO)
            SELECT
                SRC.CUSTOMER_ID,
                SRC.CUSTOMER_NAME,
                SRC.SEGMENT_KEY,
                TRUE,
                CURRENT_TIMESTAMP(),
                {self.END_DATE}
            FROM ({src_cte}) SRC
            LEFT JOIN {self.tgt_schema}.{self.config.target_table} CUR
                ON CUR.CUSTOMER_ID = SRC.CUSTOMER_ID
               AND CUR.CUSTOMER_NAME = SRC.CUSTOMER_NAME
               AND CUR.SEGMENT_KEY = SRC.SEGMENT_KEY
               AND CUR.IS_CURRENT = TRUE
            WHERE CUR.CUSTOMER_KEY IS NULL
        """
        self.conn.execute(insert_query, fetch=False)


class DateLoader(BaseDimensionLoader):
    """
    Loader for TGT_D_DATE dimension table.

    Generates a date dimension calendar instead of loading from staging.
    """

    START_DATE = "2022-01-01"
    END_DATE_CALENDAR = "2026-12-31"

    @property
    def name(self) -> str:
        return "DATE_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="",  # Not used - generated dates
            temp_table="",  # Not used
            target_table="TGT_D_DATE",
            staging_columns=[],
            temp_columns=[],
            target_columns=[
                "DATE_KEY",
                "FULL_DATE",
                "YEAR",
                "QUARTER",
                "MONTH",
                "MONTH_NAME",
                "WEEK",
                "DAY",
                "DAY_OF_WEEK",
                "DAY_NAME",
                "IS_WEEKEND",
            ],
            business_keys=["DATE_KEY"],
            change_detection_columns=[],
        )

    def _truncate_temp_table(self) -> None:
        """Override - truncate target table for date dimension."""
        self.conn.truncate_table(self.tgt_schema, self.config.target_table)

    def _load_to_temp(self) -> None:
        """Not used for date dimension."""
        pass

    def _process_changes(self) -> None:
        """Generate and insert date dimension records."""
        query = f"""
            INSERT INTO {self.tgt_schema}.{self.config.target_table}
            SELECT
                TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS DATE_KEY,
                d AS FULL_DATE,
                YEAR(d) AS YEAR,
                QUARTER(d) AS QUARTER,
                MONTH(d) AS MONTH,
                TO_CHAR(d, 'Mon') AS MONTH_NAME,
                WEEK(d) AS WEEK,
                DAY(d) AS DAY,
                DAYOFWEEK(d) AS DAY_OF_WEEK,
                TO_CHAR(d, 'Dy') AS DAY_NAME,
                IFF(DAYOFWEEK(d) IN (0, 6), TRUE, FALSE) AS IS_WEEKEND
            FROM (
                SELECT DATEADD(DAY, SEQ4(), '{self.START_DATE}'::DATE) AS d
                FROM TABLE(GENERATOR(ROWCOUNT => 4000))
            )
            WHERE d <= '{self.END_DATE_CALENDAR}'::DATE
        """
        self.conn.execute(query, fetch=False)
