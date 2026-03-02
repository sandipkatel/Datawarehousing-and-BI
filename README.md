# Data Warehouse ETL Pipeline

ETL pipeline to load data into a Snowflake data warehouse supporting both **Star Schema** and **Snowflake Schema** designs with **SCD Type 2** (Slowly Changing Dimensions).

## Schema Types

| Schema | Description |
|--------|-------------|
| **Star Schema** | Denormalized design with dimension tables directly connected to fact table. Faster queries, simpler joins. |
| **Snowflake Schema** | Normalized design with dimension tables split into sub-dimensions. Less redundancy, more complex joins. |

## What Does This Do?

This pipeline:
1. Reads data from **staging tables** (STG)
2. Loads into **temporary tables** (TMP) for processing
3. Applies changes to **target dimension & fact tables** (TGT)

It handles **SCD Type 2** (Slowly Changing Dimensions) - tracking historical changes in dimension tables with:
- `IS_CURRENT` flag to identify active records
- `EFFECTIVE_FROM` / `EFFECTIVE_TO` timestamps for versioning

## Project Structure

```
.
├── README.md
├── config.json
├── ddl/
│   └── ddl_file.sql
├── requirements.txt
├── sample.config.json
└── src/
    ├── src/
    │    ├── snowflake/         # Snowflake Schema
    │    └── star/              # Star Schema
    └── snowflake_etl/          # Professional ETL framework for SCD2
        ├── core/               # Config, DB connection, logging
        ├── loaders/            # Dimension & fact loaders
        ├── main.py             # CLI entry point
        └── orchestrator.py     # Pipeline orchestration
```

## Setup

### 1. Install Python Package

```bash
pip install snowflake-connector-python
```

### 2. Create Config File

Create `config.json` in the project root:

```json
{
  "account": "YOUR_SNOWFLAKE_ACCOUNT",
  "user": "YOUR_USERNAME",
  "password": "YOUR_PASSWORD",
  "warehouse": "COMPUTE_WH",
  "database": "YOUR_DATABASE",
  "role": "ACCOUNTADMIN",
  "schema": "PUBLIC",
  "log_path": "./logs",
  "LND_SCHEMA": "LND",
  "FILE_STAGE": "SALES_STAGE",
  "STG_SCHEMA": "STG",
  "TMP_SCHEMA": "TMP",
  "TGT_SCHEMA": "TGT"
}
```

## How to Run

Navigate to the `src` folder, then run:

```bash
# Run full pipeline (all dimensions + facts)
python -m snowflake_etl.main --config config.json

# Run only dimension tables
python -m snowflake_etl.main --config config.json --dimensions-only

# Run only fact tables
python -m snowflake_etl.main --config config.json --facts-only

# Run a specific loader
python -m snowflake_etl.main --config config.json --loader CustomerLoader
```

## Load Order

The pipeline loads tables in this order (respecting dependencies):

**Dimensions (no dependencies):**
- Country, Region, State, Category, Segment, Date

**Dimensions (with dependencies):**
- City (needs State)
- Subcategory (needs Category)
- Customer (needs Segment)
- Product (needs Subcategory)

**Fact Table (needs all dimensions):**
- FactSales

## Logs

Logs are saved to `./logs/` folder with timestamps.