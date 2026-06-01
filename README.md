# Healthcare ETL Pipeline

Production-ready ETL pipeline for processing healthcare data (Clinical Care Documents and Claims) on Databricks using PySpark and Delta Lake.

## Overview

This pipeline processes healthcare data through a medallion architecture (Bronze → Silver → Gold), transforming raw clinical documents and claims into analytical-ready patient profiles.

**Key Features:**
- CCD XML processing with nested structure flattening
- Claims CSV integration
- Patient master profile generation
- Comprehensive error handling and logging
- Orchestrated execution with detailed monitoring

## Architecture

```
Bronze Layer (Raw Ingestion)
  ├─ brnz_ccd (CCD XML files)
  └─ brnz_claims (Claims CSV files)
  |_ brnz_rx (RX Claims CSV Files)
          ↓
Silver Layer (Transformed)
  ├─ silver_problems (Patient conditions)
  └─ silver_medications (Patient medications)
          ↓
Gold Layer (Analytics)
  └─ gold_patient_master (Comprehensive patient profiles)
```

## Project Structure

```
etl/
├── ingestion/
│   └── Bronze/
│       └── load_raw_file             # Bronze layer ingestion
├── transformation/
│   ├── Silver/
│   │   ├── transform_problems        # Extract problems from CCD
│   │   └── transform_medications     # Extract medications from CCD
│   └── Gold/
│       └── consumption                # Patient master profile
├── main_caller                        # Pipeline orchestrator
└── README.md
```

## Prerequisites

- Databricks Runtime 13.0+ (Spark 3.4+)
- Python 3.10+
- Unity Catalog enabled
- Delta Lake



### 2. Prepare Data

Place your data files in Unity Catalog volumes:

```
/Volumes/workspace/default/hs/
  ├── 005ao54m-c566-7671-19c0-596o1os969x4_055d57c0a288fb38b46a4cf1431ef42345b9f60b_masked.xml
  ├── data_engineer_exam_rx_final.csv
  ├── data_engineer_exam_claims_final.csv
  
```

### 3. Update Paths

If your repository structure differs, update paths in `main_caller`:

## Usage

### Run Complete Pipeline

Execute the `main_caller` notebook:

`

### Table Names

All table names are configurable via function parameters:

```python
# Bronze layer
load_ccd_data(table_name="brnz_ccd")
load_claims_data(table_name="brnz_claims")
load_rx_data(table_name="brnz_rx")

# Silver layer
transform_problems_pipeline(target_table="silver_problems")
extract_medications_from_ccd(target_table="silver_medications")

# Gold layer
consumption_pipeline(target_table="gold_patient_insights")
```

**Built with ❤️ on Databricks**