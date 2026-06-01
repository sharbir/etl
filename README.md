# Healthcare ETL Pipeline

A production-ready ETL pipeline for processing healthcare data (Clinical Care Documents and Claims) on Databricks using PySpark and Delta Lake.

## 📋 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Flow](#data-flow)
- [Configuration](#configuration)
- [Error Handling](#error-handling)
- [Logging](#logging)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This ETL pipeline processes healthcare data through a three-tier medallion architecture (Bronze → Silver → Gold), transforming raw clinical documents and claims data into analytical-ready patient profiles.

### Key Capabilities

- **CCD Processing**: Extracts problems and medications from HL7 Clinical Care Documents (XML)
- **Claims Integration**: Processes healthcare claims data (CSV)
- **Patient Profiles**: Generates comprehensive patient master profiles combining clinical and financial data
- **Delta Lake**: All data stored in Delta format for ACID transactions and time travel
- **Production-Ready**: Comprehensive error handling, logging, and monitoring

## 🏗️ Architecture

### Medallion Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER                            │
│                   (Raw Data Ingestion)                      │
├─────────────────────────────────────────────────────────────┤
│  • brnz_ccd (CCD XML files)                                │
│  • brnz_claims (Claims CSV files)                          │
└────────────────┬────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                     SILVER LAYER                            │
│              (Cleaned & Transformed Data)                   │
├─────────────────────────────────────────────────────────────┤
│  • silver_problems (Patient problems/conditions)            │
│  • silver_medications (Patient medications)                 │
└────────────────┬────────────────────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────────────────┐
│                      GOLD LAYER                             │
│                (Analytics-Ready Data)                       │
├─────────────────────────────────────────────────────────────┤
│  • gold_patient_master (Comprehensive patient profiles)     │
└─────────────────────────────────────────────────────────────┘
```

### Pipeline Components

1. **Bronze Layer** (`load_raw_file`)
   - Ingests raw CCD XML files
   - Ingests raw Claims CSV files
   - Minimal transformation, preserves source structure

2. **Silver Layer**
   - `transform_problems`: Extracts and normalizes problem/condition data
   - `transform_medications`: Extracts and normalizes medication data

3. **Gold Layer** (`consumption`)
   - Joins claims, problems, and medications
   - Creates patient master profile with comprehensive view

4. **Orchestration** (`main_caller`)
   - Executes full pipeline in sequence
   - Handles dependencies and error propagation


Place your data files in Unity Catalog volumes:

```
/Volumes/workspace/default/hs/
  ├── sample_ccd_100_records.xml
  └── claims_data.csv
```

### 4. Create Schemas

```sql
CREATE SCHEMA IF NOT EXISTS workspace.default;
```

## 💻 Usage

### Run Complete Pipeline

Execute the `main_caller` notebook:


## 📁 Project Structure

```
etl/
├── ingestion/
│   └── Bronze/
│       └── load_raw_file         # Bronze layer ingestion
├── transformation/
│   ├── Silver/
│   │   ├── transform_problems    # Extract problems from CCD
│   │   └── transform_medications # Extract medications from CCD
│   └── Gold/
│       └── consumption            # Patient master profile
├── orchestration/
│   └── main_caller               # Pipeline orchestrator
└── README.md                     # This file
```


## ⚙️ Configuration

### Table Names

All table names are configurable via function parameters:

```python
# Bronze layer
load_ccd_data(table_name="brnz_ccd")
load_claims_data(table_name="brnz_claims")

# Silver layer
transform_problems_pipeline(target_table="silver_problems")
transform_medications_pipeline(target_table="silver_medications")

# Gold layer
consumption_pipeline(target_table="gold_patient_master")
```

### File Paths

```python
# Update in load_raw_file notebook
load_ccd_data(file_path="/Volumes/<catalog>/<schema>/<volume>/ccd_file.xml")
load_claims_data(file_path="/Volumes/<catalog>/<schema>/<volume>/claims_file.csv")
```

### Write Modes

```python
# Overwrite (default)
save_to_table(mode="overwrite")

# Append to existing table
save_to_table(mode="append")

# Error if table exists
save_to_table(mode="error")
```


**Built with ❤️ on Databricks**