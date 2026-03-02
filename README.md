# Snowflake ETL Pipeline

ETL pipeline to load data into a Snowflake data warehouse using the **Snowflake Schema** design.

## What Does This Do?

This pipeline:
1. Reads data from **staging tables** (STG)
2. Loads into **temporary tables** (TMP) for processing
3. Applies changes to **target dimension & fact tables** (TGT)

It handles **SCD Type 2** (Slowly Changing Dimensions) - tracking historical changes in dimension tables.

## Project Structure

```
.
├── README.md
├── config.json
├── ddl
│   └── ddl_file.sql
├── requirements.txt
├── sample.config.json
├── src
│   └── snowflake_etl
│       ├── __init__.py
│       ├── core
│       │   ├── __init__.py
│       │   ├── config.py
│       │   ├── database.py
│       │   ├── exceptions.py
│       │   └── logger.py
│       ├── loaders
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── dimension.py
│       │   └── fact.py
│       ├── main.py
│       └── orchestrator.py
└── venv
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
  "stg_schema": "STG",
  "tmp_schema": "TMP",
  "tgt_schema": "TGT",
  "log_path": "./logs"
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
