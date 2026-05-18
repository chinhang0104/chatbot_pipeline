"""
# Configuration Management Guide

## Overview

The chatbot pipeline uses centralized configuration management to:
- Eliminate hardcoded values
- Ensure consistency across all pipeline files
- Make changes easier (update once, apply everywhere)
- Improve maintainability and documentation

All configuration is defined in `config.py` at the pipeline root.

## Configuration Structure

```
config.py
├── CatalogConfig       # Unity Catalog settings (catalog, schema)
├── TableNames          # All table names by layer
├── SchemaConfig        # JSON schemas for parsing
├── MaintenanceConfig   # Table maintenance settings
└── PipelineConfig      # General pipeline settings
```

## Usage Examples

### 1. Accessing Catalog and Schema

```python
from config import CatalogConfig

# Get catalog and schema
catalog = CatalogConfig.CATALOG          # "main"
schema = CatalogConfig.SCHEMA            # "chat_history"

# Get fully qualified table name
full_name = CatalogConfig.get_full_table_name("bronze_checkpoints")
# Returns: "main.chat_history.bronze_checkpoints"
```

### 2. Using Table Names

```python
from config import TableNames, CatalogConfig

# Access specific table names
bronze_table = TableNames.BRONZE_CHECKPOINTS
silver_table = TableNames.SILVER_CHECKPOINTS
gold_table = TableNames.GOLD_USER_TEAM_METRICS

# Get all tables
all_tables = TableNames.get_all_tables()
# Returns: ["bronze_checkpoints", "silver_checkpoints", ...]

# Get all fully qualified table names
qualified_tables = TableNames.get_all_qualified_tables()
# Returns: ["main.chat_history.bronze_checkpoints", ...]
```

### 3. Using Schema Definitions

```python
from pyspark.sql import functions as F
from config import SchemaConfig

# Use in transformations
df = (
    bronze_df
    .withColumn("checkpoint_parsed", 
                F.from_json(F.col("checkpoint"), SchemaConfig.CHECKPOINT_SCHEMA))
    .withColumn("metadata_parsed", 
                F.from_json(F.col("metadata"), SchemaConfig.METADATA_SCHEMA))
)
```

### 4. Using Maintenance Configuration

```python
from config import MaintenanceConfig

# Get retention period
retention = MaintenanceConfig.VACUUM_RETENTION_HOURS  # 168 hours (7 days)

# Get Z-order columns for a table
zorder_cols = MaintenanceConfig.ZORDER_COLUMNS["silver_checkpoints"]
# Returns: ["thread_id", "checkpoint_date"]

# Get table properties
props = MaintenanceConfig.get_table_properties()
# Returns: {'delta.autoOptimize.optimizeWrite': 'true', ...}
```

### 5. Using Pipeline Configuration

```python
from config import PipelineConfig

# Access pipeline settings
serverless = PipelineConfig.SERVERLESS            # True
photon = PipelineConfig.PHOTON_ENABLED            # True
action = PipelineConfig.EXPECTATIONS_ACTION       # "quarantine"
```

## Files Using Configuration

### ✅ Updated Files

1. **transformations/transforms.py**
   - Uses `SchemaConfig` for JSON parsing
   - Eliminates hardcoded schema definitions

2. **utilities/maintenance.py**
   - Uses `CatalogConfig` for table names
   - Uses `TableNames` for table list
   - Uses `MaintenanceConfig` for optimization settings

3. **transformations/__init__.py**
   - Exports all transformation functions
   - Makes imports cleaner

### 📝 How to Update Your Pipeline Files

**Before (hardcoded):**
```python
# DON'T DO THIS
checkpoint_schema = StructType([
    StructField("ts", StringType(), True),
    StructField("v", StringType(), True),
    ...
])
table_name = "main.chat_history.bronze_checkpoints"
```

**After (using config):**
```python
# DO THIS
from config import SchemaConfig, CatalogConfig, TableNames

# Use predefined schemas
checkpoint_schema = SchemaConfig.CHECKPOINT_SCHEMA

# Use table name helpers
table_name = CatalogConfig.get_full_table_name(TableNames.BRONZE_CHECKPOINTS)
```

## Adding New Configuration

### Add a New Table

Edit `config.py`:

```python
class TableNames:
    # ... existing tables ...
    NEW_TABLE = "my_new_table"
    
    @classmethod
    def get_all_tables(cls) -> list:
        return [
            # ... existing tables ...
            cls.NEW_TABLE
        ]
```

### Add a New Schema

Edit `config.py`:

```python
class SchemaConfig:
    # ... existing schemas ...
    
    NEW_SCHEMA = StructType([
        StructField("field1", StringType(), True),
        StructField("field2", IntegerType(), True)
    ])
```

### Add a New Configuration Class

Edit `config.py`:

```python
class NewConfig:
    \"\"\"Description of this configuration.\"\"\"
    
    SETTING_1 = "value1"
    SETTING_2 = 42
    
    @classmethod
    def helper_method(cls):
        return f"{cls.SETTING_1}_{cls.SETTING_2}"
```

## Maintenance Scripts

### Run Maintenance (Safe Operations)

```python
# Import and run
from utilities.maintenance import run_all_maintenance

# Run with default settings (OPTIMIZE only)
run_all_maintenance()

# Preview commands first (dry-run)
run_all_maintenance(dry_run=True)

# Include table analysis (slower but thorough)
run_all_maintenance(include_analysis=True)
```

### Individual Operations

```python
from utilities.maintenance import (
    optimize_all_tables,
    configure_all_tables,
    analyze_all_tables
)

# Run specific operations
optimize_all_tables()          # OPTIMIZE with Z-ordering
configure_all_tables()         # Set table properties
analyze_all_tables()           # Compute statistics
```

## Benefits of Configuration Management

✅ **Single Source of Truth**
   - All settings in one place
   - No duplicate definitions

✅ **Easy Updates**
   - Change catalog/schema once
   - Automatically applies everywhere

✅ **Type Safety**
   - Schema definitions validated
   - Reduces runtime errors

✅ **Documentation**
   - Clear structure
   - Easy to understand

✅ **Testing**
   - Mock configurations easily
   - Swap test vs production configs

✅ **Maintainability**
   - Find settings quickly
   - Refactor with confidence

## Next Steps

1. Review `config.py` to understand all available settings
2. Update remaining pipeline files to use config
3. Add new tables/schemas to config as needed
4. Use maintenance scripts for regular optimization
5. Consider environment-specific configs (dev/prod)
"""
