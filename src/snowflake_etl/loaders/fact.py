"""
Fact table loaders for the Snowflake schema.

Implements MERGE operations for fact tables with automatic dimension key lookups.
"""

from snowflake_etl.loaders.base import BaseFactLoader, LoaderConfig


class FactSalesLoader(BaseFactLoader):
    """
    Loader for TGT_F_SALES_SF fact table.

    Performs dimension key lookups and MERGE operations
    for the sales fact table.
    """

    @property
    def name(self) -> str:
        return "FACT_SALES_LOAD"

    @property
    def config(self) -> LoaderConfig:
        return LoaderConfig(
            staging_view="STG_F_SALES",
            temp_table="TMP_F_SALES_SF",
            target_table="TGT_F_SALES_SF",
            staging_columns=[
                "ORDER_ID",
                "CUSTOMER_ID",
                "PRODUCT_ID",
                "CITY",
                "POSTAL_CODE",
                "ORDER_DATE",
                "SHIP_DATE",
                "SHIP_MODE",
                "QUANTITY",
                "DISCOUNT",
                "REVENUE",
                "PROFIT",
                "COST",
            ],
            temp_columns=[
                "ORDER_ID",
                "CUSTOMER_ID",
                "PRODUCT_ID",
                "CITY_NAME",
                "POSTAL_CODE",
                "ORDER_DATE",
                "SHIP_DATE",
                "SHIP_MODE",
                "QUANTITY",
                "DISCOUNT",
                "REVENUE",
                "PROFIT",
                "COST",
            ],
            target_columns=[
                "ORDER_ID",
                "CUSTOMER_KEY",
                "PRODUCT_KEY",
                "CITY_KEY",
                "ORDER_DATE_KEY",
                "SHIP_DATE_KEY",
                "SHIP_MODE_KEY",
                "QUANTITY",
                "DISCOUNT",
                "REVENUE",
                "PROFIT",
                "COST",
            ],
            business_keys=["ORDER_ID"],
            change_detection_columns=["QUANTITY", "DISCOUNT", "REVENUE", "PROFIT", "COST"],
        )

    def _load_to_temp(self) -> None:
        """Load aggregated sales data from staging to temp table."""
        query = f"""
            INSERT INTO {self.tmp_schema}.{self.config.temp_table}
            (ORDER_ID, CUSTOMER_ID, PRODUCT_ID, CITY_NAME, POSTAL_CODE,
             ORDER_DATE, SHIP_DATE, SHIP_MODE, QUANTITY, DISCOUNT, REVENUE, PROFIT, COST)
            SELECT
                SRC.ORDER_ID,
                SRC.CUSTOMER_ID,
                SRC.PRODUCT_ID,
                SRC.CITY,
                SRC.POSTAL_CODE,
                SRC.ORDER_DATE,
                SRC.SHIP_DATE,
                SRC.SHIP_MODE,
                SUM(SRC.QUANTITY),
                SUM(SRC.DISCOUNT),
                SUM(SRC.REVENUE),
                SUM(SRC.PROFIT),
                SUM(SRC.COST)
            FROM {self.stg_schema}.{self.config.staging_view} SRC
            GROUP BY
                SRC.ORDER_ID,
                SRC.CUSTOMER_ID,
                SRC.PRODUCT_ID,
                SRC.CITY,
                SRC.POSTAL_CODE,
                SRC.ORDER_DATE,
                SRC.SHIP_DATE,
                SRC.SHIP_MODE
        """
        self.conn.execute(query, fetch=False)

    def _process_changes(self) -> None:
        """Execute MERGE with dimension key lookups."""
        # Source CTE with all dimension key lookups
        src_cte = f"""
            SELECT
                TMP.ORDER_ID,
                CUS.CUSTOMER_KEY,
                PRD.PRODUCT_KEY,
                CTY.CITY_KEY,
                ORD_DT.DATE_KEY AS ORDER_DATE_KEY,
                SHP_DT.DATE_KEY AS SHIP_DATE_KEY,
                SHIP.SHIP_MODE_KEY,
                TMP.QUANTITY,
                TMP.DISCOUNT,
                TMP.REVENUE,
                TMP.PROFIT,
                TMP.COST
            FROM {self.tmp_schema}.{self.config.temp_table} TMP

            INNER JOIN {self.tgt_schema}.TGT_D_CUSTOMER CUS
                ON CUS.CUSTOMER_ID = TMP.CUSTOMER_ID

            INNER JOIN {self.tgt_schema}.TGT_D_PRODUCT PRD
                ON PRD.PRODUCT_ID = TMP.PRODUCT_ID

            INNER JOIN {self.tgt_schema}.TGT_D_CITY CTY
                ON CTY.CITY_NAME = TMP.CITY_NAME
                AND CTY.POSTAL_CODE = TMP.POSTAL_CODE

            INNER JOIN {self.tgt_schema}.TGT_D_DATE ORD_DT
                ON ORD_DT.FULL_DATE = TMP.ORDER_DATE

            INNER JOIN {self.tgt_schema}.TGT_D_DATE SHP_DT
                ON SHP_DT.FULL_DATE = TMP.SHIP_DATE

            INNER JOIN {self.tgt_schema}.TGT_D_SHIP_MODE SHIP
                ON SHIP.SHIP_MODE = TMP.SHIP_MODE
        """

        merge_query = f"""
            MERGE INTO {self.tgt_schema}.{self.config.target_table} AS TGT
            USING ({src_cte}) AS SRC
                ON TGT.ORDER_ID = SRC.ORDER_ID
            WHEN MATCHED THEN
                UPDATE SET
                    TGT.QUANTITY = SRC.QUANTITY,
                    TGT.COST = SRC.COST,
                    TGT.DISCOUNT = SRC.DISCOUNT,
                    TGT.REVENUE = SRC.REVENUE,
                    TGT.PROFIT = SRC.PROFIT
            WHEN NOT MATCHED THEN
                INSERT (
                    ORDER_ID,
                    CUSTOMER_KEY,
                    PRODUCT_KEY,
                    CITY_KEY,
                    ORDER_DATE_KEY,
                    SHIP_DATE_KEY,
                    SHIP_MODE_KEY,
                    QUANTITY,
                    DISCOUNT,
                    REVENUE,
                    PROFIT,
                    COST
                )
                VALUES (
                    SRC.ORDER_ID,
                    SRC.CUSTOMER_KEY,
                    SRC.PRODUCT_KEY,
                    SRC.CITY_KEY,
                    SRC.ORDER_DATE_KEY,
                    SRC.SHIP_DATE_KEY,
                    SRC.SHIP_MODE_KEY,
                    SRC.QUANTITY,
                    SRC.DISCOUNT,
                    SRC.REVENUE,
                    SRC.PROFIT,
                    SRC.COST
                )
        """
        self.conn.execute(merge_query, fetch=False)
