# Healthcare ETL Pipeline

Production-ready ETL pipeline for processing healthcare data (Clinical Care Documents and Claims) on Databricks using PySpark and Delta Lake.

## Overview

This pipeline processes healthcare data through a medallion architecture (Bronze → Silver → Gold), transforming raw clinical documents and claims into analytical-ready patient profiles.

**Key Features:**
- CCD XML processing with nested structure flattening
- Claims CSV integration
- Patient master profile generation
- Delta Lake for ACID transactions
- Comprehensive error handling and logging
- Orchestrated execution with detailed monitoring

## Architecture

```
Bronze Layer (Raw Ingestion)
  ├─ brnz_ccd (CCD XML files)
  └─ brnz_claims (Claims CSV files)
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

## Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd etl
```

### 2. Prepare Data

Place your data files in Unity Catalog volumes:

```
/Volumes/workspace/default/hs/
  ├── sample_ccd_100_records.xml
  └── claims_data.csv
```

### 3. Update Paths

If your repository structure differs, update paths in `main_caller`:

```python
pipeline_stages = [
    {"path": "/Repos/<your-email>/etl/ingestion/Bronze/load_raw_file"},
    ...
]
```

## Usage

### Run Complete Pipeline

Execute the `main_caller` notebook:

```python
# Run all stages with stop-on-error
results = execute_etl_pipeline(stop_on_error=True)

# Continue through all stages regardless of errors
results = execute_etl_pipeline(stop_on_error=False)
```

### Run Individual Stages

Each notebook can be executed independently:

```python
# Bronze: Load raw data
%run /Repos/<your-email>/etl/ingestion/Bronze/load_raw_file

# Silver: Transform problems
%run /Repos/<your-email>/etl/transformation/Silver/transform_problems

# Silver: Transform medications
%run /Repos/<your-email>/etl/transformation/Silver/transform_medications

# Gold: Create patient profiles
%run /Repos/<your-email>/etl/transformation/Gold/consumption
```

## Pipeline Stages

### 1. Bronze Layer (load_raw_file)

**Input:** Raw XML and CSV files  
**Output:** `brnz_ccd`, `brnz_claims` Delta tables  
**Functions:**
- `load_ccd_data()` - Parse XML with nested CCD structure
- `load_claims_data()` - Load CSV claims data
- `main()` - Orchestrate both loads with error handling

### 2. Silver Layer - Problems (transform_problems)

**Input:** `brnz_ccd`  
**Output:** `silver_problems`  
**Schema:** patient_id, id, code, code_system, code_system_name, problem_name  
**Key Operations:**
- Navigate nested XML structure
- Flatten arrays using `arrays_zip` and `inline`
- Extract observation data

### 3. Silver Layer - Medications (transform_medications)

**Input:** `brnz_ccd`  
**Output:** `silver_medications`  
**Schema:** patient_id, id, code_system, code_system_name, medication_name  
**Key Operations:**
- Extract medication codes from substanceAdministration
- Flatten nested consumable data

### 4. Gold Layer (consumption)

**Input:** `brnz_claims`, `silver_problems`, `silver_medications`  
**Output:** `gold_patient_master`  
**Schema:**
- patient_id
- total_claims_count
- total_allowed_financials
- total_paid_financials
- active_problems (array of structs)
- active_medications (array of structs)

**Key Operations:**
- Aggregate claims by patient
- Collect problems and medications into arrays
- FULL OUTER JOIN to preserve all patient records

## Code Quality Features

- ✓ **Type Hints** - Full typing with `Optional`, `Dict`, `Tuple`
- ✓ **Error Handling** - Try-except blocks with graceful recovery
- ✓ **Logging** - Comprehensive logging at INFO/WARNING/ERROR levels
- ✓ **Docstrings** - Google-style docstrings for all functions
- ✓ **Parameterization** - All functions accept configurable parameters
- ✓ **Data Validation** - Checks for empty results and null values

## Configuration

### Table Names

All table names are configurable via function parameters:

```python
# Bronze layer
load_ccd_data(table_name="custom_ccd")
load_claims_data(table_name="custom_claims")

# Silver layer
transform_problems_pipeline(target_table="custom_problems")
transform_medications_pipeline(target_table="custom_medications")

# Gold layer
consumption_pipeline(target_table="custom_patient_master")
```

### Write Modes

```python
# Overwrite existing data (default)
save_to_table(mode="overwrite")

# Append to existing table
save_to_table(mode="append")
```

## Error Handling

All functions follow a consistent pattern:

1. **Try-Except Blocks** - Wrap all operations
2. **Logging** - Log errors with full stack trace
3. **Return None/False** - Return indicators instead of raising
4. **Propagation** - Calling functions check return values

The orchestrator tracks success/failure per stage:

```python
{
    "status": "success",  # or "partial_success", "failed"
    "total_stages": 4,
    "successful_stages": 4,
    "failed_stages": 0,
    "stages": [
        {"name": "Bronze", "success": True, "duration": 12.34},
        ...
    ]
}
```

## Logging

**Log Levels:**
- **INFO** - Normal operations, progress updates
- **WARNING** - Non-critical issues (empty results, missing data)
- **ERROR** - Failures, exceptions

**Log Format:**
```
2026-06-01 19:35:12 - INFO - Starting patient master profile generation
2026-06-01 19:35:15 - INFO - Successfully generated 1234 patient profiles
```

## Orchestration

The `main_caller` notebook orchestrates the pipeline:

- Sequential execution with dependency management
- Configurable error handling (`stop_on_error` flag)
- Execution timing per stage
- Comprehensive summary reports
- Visual status indicators (✓ ✗ ⚠)

## Contributing

### Code Style

- Follow PEP 8
- Use type hints
- Write Google-style docstrings
- Add logging for key operations
- Handle errors gracefully (return None/False, don't raise)

### Adding New Transformations

1. Create extraction function with try-except
2. Create save function
3. Create pipeline function
4. Add to `main_caller` pipeline stages
5. Update README

## Support

For issues or questions:

1. Check existing documentation
2. Review error logs
3. Open an issue on GitHub
4. Contact the data engineering team

## Resources

- [Databricks Documentation](https://docs.databricks.com/)
- [Delta Lake Documentation](https://docs.delta.io/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [HL7 CCD Documentation](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=7)

---

**Built with ❤️ on Databricks**