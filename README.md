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
snowflake_etl/
├── main.py           # Run the pipeline from here
├── orchestrator.py   # Controls the load order
├── core/
│   ├── config.py     # Reads config.json
│   ├── database.py   # Connects to Snowflake
│   ├── logger.py     # Writes logs
│   └── exceptions.py # Error handling
└── loaders/
    ├── base.py       # Base class for all loaders
    ├── dimension.py  # Dimension loaders (Country, City, Product, etc.)
    └── fact.py       # Fact table loader (Sales)
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

## Quick Python Example

```python
from snowflake_etl.core.config import Settings
from snowflake_etl.orchestrator import ETLOrchestrator

settings = Settings.from_json("config.json")
orchestrator = ETLOrchestrator(settings)
result = orchestrator.run()

print("Success!" if result.success else "Failed!")
```

## License

MIT
