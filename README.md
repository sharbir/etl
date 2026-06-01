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

## ✨ Features

### Code Quality

- ✅ **Type Hints**: Full typing support with `Optional`, `Dict`, `List`, `Tuple`
- ✅ **Error Handling**: Try-except blocks with graceful error recovery
- ✅ **Logging**: Comprehensive logging at INFO, WARNING, and ERROR levels
- ✅ **Docstrings**: Google-style docstrings for all functions
- ✅ **Parameterized**: All functions accept configurable parameters
- ✅ **Pythonic**: Follows PEP 8 and Python best practices

### Data Processing

- ✅ **XML Parsing**: Handles nested HL7 CCD structure
- ✅ **Array Operations**: Uses `arrays_zip` and `inline` for array flattening
- ✅ **FULL OUTER JOIN**: Retains patients with incomplete data
- ✅ **Data Validation**: Checks for empty results and null values
- ✅ **Delta Lake**: ACID transactions, schema evolution, time travel

### Pipeline Features

- ✅ **Configurable**: Easy to modify source/target tables
- ✅ **Reusable**: Functions can be imported and used elsewhere
- ✅ **Stop-on-Error**: Configurable error handling strategy
- ✅ **Progress Tracking**: Detailed execution statistics and timing
- ✅ **Summary Reports**: Visual pipeline execution summaries

## 📦 Prerequisites

### Required

- Databricks Runtime 13.0+ (Spark 3.4+)
- Python 3.10+
- Unity Catalog enabled
- Delta Lake

### Recommended

- Serverless compute or cluster with Photon enabled
- At least 8GB RAM for processing

## 🚀 Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd etl
```

### 2. Configure Paths

Update the paths in `main_caller` notebook if your repo structure differs:

```python
pipeline_stages = [
    {"path": "/Repos/<your-email>/etl/ingestion/Bronze/load_raw_file"},
    {"path": "/Repos/<your-email>/etl/transformation/Silver/transform_medications"},
    {"path": "/Repos/<your-email>/etl/transformation/Silver/transform_problems"},
    {"path": "/Repos/<your-email>/etl/transformation/Gold/consumption"}
]
```

### 3. Prepare Data

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

```python
# Run all stages with stop-on-error
results = execute_etl_pipeline(stop_on_error=True)

# Run all stages regardless of errors
results = execute_etl_pipeline(stop_on_error=False)
```

### Run Individual Stages

#### Bronze Layer

```python
# Load CCD data
ccd_df = load_ccd_data(
    file_path="/Volumes/workspace/default/hs/sample_ccd_100_records.xml",
    table_name="brnz_ccd"
)

# Load Claims data
claims_df = load_claims_data(
    file_path="/Volumes/workspace/default/hs/claims_data.csv",
    table_name="brnz_claims"
)
```

#### Silver Layer

```python
# Transform problems
problems_df = transform_problems_pipeline(
    source_table="brnz_ccd",
    target_table="silver_problems",
    save_to_table=True
)

# Transform medications
medications_df = transform_medications_pipeline(
    source_table="brnz_ccd",
    target_table="silver_medications",
    save_to_table=True
)
```

#### Gold Layer

```python
# Create patient master profile
profile_df = consumption_pipeline(
    claims_table="brnz_claims",
    problems_table="silver_problems",
    medications_table="silver_medications",
    target_table="gold_patient_master",
    save_to_table=True
)
```

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

## 🔄 Data Flow

### 1. Bronze Layer (Raw Ingestion)

**Input**: 
- CCD XML files (HL7 Clinical Care Documents)
- Claims CSV files

**Output**:
- `brnz_ccd`: Raw CCD data with nested XML structure
- `brnz_claims`: Raw claims data

**Key Operations**:
- XML parsing with `rowTag="ClinicalDocument"`
- CSV parsing with header and schema inference
- Delta table creation

### 2. Silver Layer (Transformation)

#### Problems Extraction

**Input**: `brnz_ccd`

**Output**: `silver_problems`

**Schema**:
```
id: string
code: long
code_system: string
code_system_name: string
problem_name: string
```

**Key Operations**:
- Navigate nested structure: `component.structuredBody.component.section`
- Extract observation data from `entry.act.entryRelationship.observation`
- Use `arrays_zip` and `inline` to flatten arrays
- Filter null values

#### Medications Extraction

**Input**: `brnz_ccd`

**Output**: `silver_medications`

**Schema**:
```
id: string
code_system: string
code_system_name: string
medication_name: string
```

**Key Operations**:
- Navigate to `entry.substanceAdministration.consumable`
- Extract medication codes and names
- Flatten nested arrays

### 3. Gold Layer (Analytics)

**Input**: 
- `brnz_claims`
- `silver_problems`
- `silver_medications`

**Output**: `gold_patient_master`

**Schema**:
```
patient_id: string
total_claims_count: long
total_allowed_financials: double
total_paid_financials: double
active_problems: array<struct<problem_name:string>>
active_medications: array<struct<medication_name:string>>
```

**Key Operations**:
- Aggregate claims by patient
- Collect problems and medications into arrays
- FULL OUTER JOIN to preserve all patient records
- Create comprehensive patient profile

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

### Pipeline Behavior

```python
# Stop on first error (default)
execute_etl_pipeline(stop_on_error=True)

# Continue through all stages
execute_etl_pipeline(stop_on_error=False)
```

## 🛡️ Error Handling

### Error Handling Strategy

All functions follow a consistent error handling pattern:

1. **Try-Except Blocks**: Wrap all operations
2. **Logging**: Log errors with full stack trace
3. **Return None**: Return `None` on error instead of raising
4. **Caller Checks**: Calling functions check for `None` and handle appropriately

### Example Error Flow

```python
# Function returns None on error
df = extract_problems_from_ccd()

if df is None:
    logger.error("Failed to extract problems")
    return None  # Propagate failure up

# Continue processing
df = transform_data(df)
```

### Pipeline Error Handling

The main pipeline tracks success/failure for each stage:

```python
{
    "status": "partial_success",
    "total_stages": 4,
    "successful_stages": 3,
    "failed_stages": 1,
    "stages": [
        {"name": "Bronze", "success": True, "duration": 12.34},
        {"name": "Silver - Medications", "success": True, "duration": 8.56},
        {"name": "Silver - Problems", "success": False, "message": "Error..."},
        {"name": "Gold", "success": False}  # Skipped due to dependency
    ]
}
```

## 📊 Logging

### Log Levels

- **INFO**: Normal operations, progress updates
- **WARNING**: Non-critical issues (empty results, missing data)
- **ERROR**: Failures, exceptions

### Log Format

```
2026-05-31 23:45:12 - INFO - Starting patient master profile generation
2026-05-31 23:45:15 - INFO - Successfully generated 1234 patient profiles
2026-05-31 23:45:16 - ERROR - Failed to save to table: PermissionDenied
```

### Enable Debug Logging

```python
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
```

## 🤝 Contributing

### Code Style

- Follow PEP 8
- Use type hints
- Write docstrings for all functions
- Add logging for key operations
- Handle errors gracefully

### Adding New Transformations

1. Create extraction function with try-except
2. Create save function
3. Create pipeline function that orchestrates
4. Add to main_caller pipeline stages
5. Update README

### Example Template

```python
def extract_new_data(source_table: str = "brnz_source") -> Optional[DataFrame]:
    """
    Extract and transform new data.
    
    Args:
        source_table: Source table name
    
    Returns:
        DataFrame or None if error
    """
    try:
        logger.info(f"Starting extraction from {source_table}")
        
        query = f"SELECT * FROM {source_table}"
        df = spark.sql(query)
        
        if df.count() == 0:
            logger.warning("No data found")
            return None
        
        logger.info("Extraction successful")
        return df
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}", exc_info=True)
        return None
```

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 📞 Support

For issues, questions, or contributions, please:

1. Check existing documentation
2. Review error logs
3. Open an issue on GitHub
4. Contact the data engineering team

## 🎓 Additional Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [HL7 CCD Documentation](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=7)

---

**Built with ❤️ on Databricks**