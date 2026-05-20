# Chatbot Analytics Pipeline

A production-grade Lakeflow Spark Declarative Pipeline for processing chatbot checkpoint data using the Medallion architecture pattern.

## Overview

This pipeline ingests, cleanses, and aggregates chatbot checkpoint events to produce actionable user and team analytics. Built on Databricks with Unity Catalog, it implements enterprise-grade data quality, incremental processing, and change data capture patterns.

### Key Features

* **Incremental Ingestion**: Auto Loader for efficient CSV processing from cloud storage
* **Change Data Capture**: SCD Type 2 tracking for thread ownership history
* **Data Quality**: Built-in expectations with automatic row-level validation
* **Performance**: Liquid clustering, auto-optimization, and serverless compute
* **Observability**: Comprehensive monitoring queries and data profiling
* **Testing**: Unit and integration tests for transformation logic

### Pipeline Statistics

* **Layers**: 3 (Bronze → Silver → Gold)
* **Datasets**: 6 pipeline tables + 4 monitoring views
* **Compute**: Serverless with Photon acceleration
* **Catalog**: main.chat_history

## Architecture

### Data Flow

```
Cloud Storage (Volumes)
    ↓
[Bronze Layer]
    • bronze_checkpoints (Auto Loader: checkpoints_*.csv)
    • bronze_thread_lookup_stream (Auto Loader: thread_teams_*.csv)
    ↓
[Silver Layer]
    • silver_thread_lookup_history (Auto CDC: SCD Type 2)
    • silver_checkpoints (Enriched with user/team)
    ↓
[Gold Layer]
    • gold_user_team_metrics (User × Team × Date aggregations)
    • gold_team_summary (Team × Date aggregations)
```

### Medallion Layers

#### Bronze Layer
Raw data ingestion with schema evolution and error handling.

* **Purpose**: Capture source data exactly as provided
* **Pattern**: Streaming tables with Auto Loader
* **Quality**: Rescue mode for schema violations
* **Features**: Ingestion timestamps, auto-optimized writes

#### Silver Layer
Cleaned, enriched, and validated data ready for analytics.

* **Purpose**: Business-ready datasets with quality guarantees
* **Pattern**: Auto CDC for dimensional data, materialized views for facts
* **Quality**: Constraint-based expectations (drop invalid rows)
* **Features**: JSON parsing, type casting, dimension enrichment

#### Gold Layer
Business-level aggregations and metrics for reporting.

* **Purpose**: Pre-computed analytics optimized for query performance
* **Pattern**: Materialized views with liquid clustering
* **Quality**: Aggregate-level validation
* **Features**: Daily rollups, team hierarchies, performance metrics

## Data Sources

### Checkpoints CSV
* **Location**: `/Volumes/main/chat_history/raw_data/checkpoints_*.csv`
* **Pattern**: Incremental append-only files
* **Frequency**: Continuous/batch (controlled by file arrival)
* **Schema**:
  * `thread_id` (string): Conversation thread identifier
  * `checkpoint_ns` (string): Checkpoint namespace
  * `checkpoint_id` (string): Unique checkpoint identifier
  * `parent_checkpoint_id` (string): Parent checkpoint reference
  * `type` (string): Checkpoint event type
  * `checkpoint` (JSON string): Checkpoint metadata (ts, v, id, channel_versions)
  * `metadata` (JSON string): Step metadata (step, source)

### Thread Lookup CSV
* **Location**: `/Volumes/main/chat_history/raw_data/thread_teams_*.csv`
* **Pattern**: Full snapshot files (SCD Type 2 via Auto CDC)
* **Frequency**: On ownership change
* **Schema**:
  * `thread_id` (string): Primary key
  * `user_id` (string): Thread owner
  * `team` (string): Team affiliation
* **Change Detection**: Auto CDC tracks full history with `__START_AT` and `__END_AT`

## Datasets

### Bronze Layer (`transformations/bronze.py`)

#### bronze_checkpoints
* **Type**: Streaming Table
* **Source**: Auto Loader (cloudFiles)
* **File Pattern**: `checkpoints_*.csv`
* **Schema Handling**: Rescue mode with `_rescued_data` column
* **Features**:
  * Incremental CSV ingestion
  * Schema hints for type enforcement
  * Automatic schema evolution
  * Ingestion timestamp tracking
* **Table Properties**: Auto-optimize writes enabled

#### bronze_thread_lookup_stream
* **Type**: Streaming Table
* **Source**: Auto Loader (cloudFiles)
* **File Pattern**: `thread_teams_*.csv`
* **Features**:
  * Incremental CSV ingestion
  * Source for downstream CDC processing
  * Ingestion timestamp tracking

### Silver Layer (`transformations/silver.py`)

#### thread_lookup_cleaned
* **Type**: Temporary View
* **Source**: `bronze_thread_lookup_stream` (streaming read)
* **Purpose**: Preprocessing step before Auto CDC
* **Transformations**:
  * Type casting to string
  * Adds `updated_at` timestamp for CDC sequencing
* **Quality**: Drops rows where `thread_id IS NULL`

#### silver_thread_lookup_history
* **Type**: Streaming Table (Auto CDC target)
* **Source**: `thread_lookup_cleaned` via Auto CDC flow
* **Pattern**: SCD Type 2 (Slowly Changing Dimension Type 2)
* **Keys**: `thread_id`
* **Sequence**: `updated_at`
* **Features**:
  * Full change history tracking
  * Temporal columns: `__START_AT`, `__END_AT`
  * Query current state: `WHERE __END_AT IS NULL`
  * Query history: Use temporal columns for point-in-time queries
* **Use Case**: Track thread ownership changes over time

#### silver_checkpoints
* **Type**: Materialized View
* **Source**: `bronze_checkpoints` ⋈ `silver_thread_lookup_history` (current records)
* **Join**: Left join on `thread_id` (preserves checkpoints without lookup)
* **Transformations**:
  * Parse `checkpoint` JSON → extract `ts`, `v`, `id`, `channel_versions`
  * Parse `metadata` JSON → extract `step`, `source`
  * Type casting: `checkpoint_v` (int), `metadata_step_int` (int)
  * Temporal columns: `checkpoint_date`, `checkpoint_hour`
* **Quality**: Drops rows where `checkpoint_ts IS NULL`
* **Output Columns**:
  * Source: `thread_id`, `checkpoint_ns`, `checkpoint_id`, `parent_checkpoint_id`, `type`
  * Parsed: `checkpoint_ts`, `checkpoint_date`, `checkpoint_hour`, `checkpoint_v`, `checkpoint_id_parsed`, `metadata_step_int`, `metadata_source`, `channel_versions_start`
  * Enriched: `user_id`, `team`

### Gold Layer (`transformations/gold.py`)

#### gold_user_team_metrics
* **Type**: Materialized View
* **Source**: `silver_checkpoints`
* **Granularity**: Per user, per team, per day
* **Clustering**: Liquid clustering by `[checkpoint_date, team, user_id]`
* **Metrics**:
  * `checkpoint_count`: Total checkpoints
  * `thread_count`: Distinct threads per user
  * `unique_checkpoints`: Distinct checkpoint IDs
  * `first_checkpoint_ts`, `last_checkpoint_ts`: Time range
  * `min_step`, `max_step`, `avg_step`: Step statistics
* **Quality Constraints**:
  * `checkpoint_date IS NOT NULL`
  * `user_id IS NOT NULL`
  * `team IS NOT NULL`
* **Use Case**: User activity analysis, productivity tracking

#### gold_team_summary
* **Type**: Materialized View
* **Source**: `silver_checkpoints`
* **Granularity**: Per team, per day
* **Clustering**: Liquid clustering by `[checkpoint_date, team]`
* **Metrics**:
  * `checkpoint_count`: Total team checkpoints
  * `user_count`: Distinct users per team
  * `thread_count`: Distinct threads
  * `unique_checkpoints`: Distinct checkpoint IDs
  * `first_checkpoint_ts`, `last_checkpoint_ts`: Time range
  * `min_step`, `max_step`, `avg_step`: Step statistics
* **Quality Constraints**:
  * `checkpoint_date IS NOT NULL`
  * `team IS NOT NULL`
* **Use Case**: Team performance dashboards, capacity planning

## Directory Structure

```
chatbot_pipeline/
├── README.md                       # This file
├── config.py                       # Centralized configuration
├── CONFIGURATION.py                # Legacy config (deprecated)
│
├── transformations/                # Pipeline source code
│   ├── __init__.py
│   ├── transforms.py               # Pure transformation functions
│   ├── bronze.py                   # Bronze layer datasets
│   ├── silver.py                   # Silver layer datasets
│   └── gold.py                     # Gold layer datasets
│
├── monitoring/                     # Data profiling and monitoring
│   └── data_profiling_queries.sql  # Monitoring SQL queries
│
├── utilities/                      # Maintenance and utilities
│   ├── maintenance.py              # Table maintenance operations
│   ├── vacuum_tables.py            # VACUUM automation
│   ├── history.sql                 # Historical queries
│   ├── restore.sql                 # Data restoration scripts
│   └── team_daily.sql              # Team daily reports
│
└── tests/                          # Test suite
    ├── __init__.py
    ├── conftest.py                 # Pytest configuration
    ├── run_tests.py                # Test runner
    ├── test_silver.py              # Silver layer tests
    ├── test_gold.py                # Gold layer tests
    ├── test_integration.py         # End-to-end tests
    └── README.py                   # Test documentation
```

## Configuration

The pipeline uses a centralized configuration module (`config.py`) for maintainability.

### Key Configuration Classes

* **CatalogConfig**: Unity Catalog paths (`main.chat_history`)
* **TableNames**: Standardized table name constants
* **SchemaConfig**: JSON parsing schemas for checkpoints and metadata
* **MaintenanceConfig**: VACUUM, Z-ORDER, liquid clustering, table properties
* **PipelineConfig**: Serverless settings, Photon, trigger intervals

### Configuration Highlights

```python
from config import CatalogConfig, TableNames, MaintenanceConfig

# Get fully qualified table name
table_name = CatalogConfig.get_full_table_name(TableNames.GOLD_USER_TEAM_METRICS)
# Returns: "main.chat_history.gold_user_team_metrics"

# Get recommended table properties for Gold layer
props = MaintenanceConfig.get_table_properties(TableNames.GOLD_TEAM_SUMMARY)
# Returns: auto-optimize, deletion vectors, clustering configs

# Check if table should use liquid clustering
should_cluster = MaintenanceConfig.should_use_liquid_clustering(table_name)
```

## Transformation Logic

## Related Projects and Integration

This project is part of a broader chatbot analytics ecosystem.

* **LangGraph analysis**: This project analyzes data generated by LangGraph.
  * Repository: https://github.com/chinhang0104/LangGraph-deploy
* **Chatbot dbt baseline**: An additional project builds on top of this pipeline.
  * Repository: https://github.com/chinhang0104/chatbot_dbt

## Example: Using Transformations and Tests for Chat-History Data

This repository provides Python-based transformation logic and test coverage for chat-history data used by the chatbot pipeline and downstream analytics.

* Use `transformations/bronze.py`, `transformations/silver.py`, and `transformations/gold.py` to define the bronze/silver/gold data flow.
* Use `transformations/transforms.py` for reusable transformation functions and parsing logic.
* Use tests in `tests/test_silver.py`, `tests/test_gold.py`, and `tests/test_integration.py` to validate pipeline behavior.

Example workflow:

1. Ingest raw checkpoint and thread lookup data into the bronze layer.
2. Enrich and validate chat-history records in the silver layer.
3. Aggregate user/team metrics in the gold layer for downstream analytics.
4. Run the included tests to confirm transformation logic and analytics results.

### dbt Compatibility Note

* The current pipeline includes Python-based transformations that are not supported for local execution with dbt alone.
* This repository is intended to demonstrate the same chat-history analytics patterns in Python, while the `chatbot_dbt` project provides a dbt-centric implementation for supported scenarios.
* For local dbt workflows, the Python transformations in this repo may require alternative execution or adaptation before they can run in a pure dbt environment.


Pure transformation functions in `transformations/transforms.py`:

* **clean_thread_lookup**: Standardizes thread lookup data and adds `updated_at`
* **enrich_checkpoints**: Parses JSON, extracts fields, joins with dimension table
* **gold_user_team_metrics**: Aggregates to user × team × date grain
* **gold_team_summary**: Aggregates to team × date grain

These functions are pure (no side effects) and unit-testable outside the pipeline context.

## Usage

### Running the Pipeline

**Dry Run** (validate without executing):
```python
# From pipeline editor page
Start dry run
```

**Incremental Update** (process new data only):
```python
# From pipeline editor page
Start pipeline update
```

**Full Refresh** (reprocess all data - expensive):
```python
# From pipeline editor page
Start full refresh
# ⚠️ Warning: Drops and recreates all tables
```

**Selective Refresh** (specific dataset):
```python
# From pipeline editor page
Start pipeline update with datasets: ["gold_team_summary"]
```

### Querying the Data

#### Daily User Metrics
```sql
SELECT 
  checkpoint_date,
  user_id,
  team,
  checkpoint_count,
  thread_count,
  avg_step
FROM main.chat_history.gold_user_team_metrics
WHERE checkpoint_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY checkpoint_date DESC, checkpoint_count DESC;
```

#### Team Performance Dashboard
```sql
SELECT 
  checkpoint_date,
  team,
  user_count,
  thread_count,
  checkpoint_count,
  ROUND(checkpoint_count / user_count, 2) AS avg_checkpoints_per_user
FROM main.chat_history.gold_team_summary
WHERE checkpoint_date >= CURRENT_DATE() - INTERVAL 30 DAYS
ORDER BY team, checkpoint_date;
```

#### Thread Ownership History (SCD Type 2)
```sql
-- Current thread ownership
SELECT thread_id, user_id, team
FROM main.chat_history.silver_thread_lookup_history
WHERE __END_AT IS NULL;

-- Historical changes for specific thread
SELECT 
  thread_id,
  user_id,
  team,
  __START_AT AS valid_from,
  __END_AT AS valid_to
FROM main.chat_history.silver_thread_lookup_history
WHERE thread_id = 'specific-thread-id'
ORDER BY __START_AT;
```

#### Data Quality Monitoring
```sql
-- Check for rescued data in Bronze
SELECT COUNT(*) AS rescued_row_count
FROM main.chat_history.bronze_checkpoints
WHERE _rescued_data IS NOT NULL;

-- Checkpoint volume by date
SELECT 
  checkpoint_date,
  COUNT(*) AS row_count,
  COUNT(DISTINCT thread_id) AS distinct_threads
FROM main.chat_history.silver_checkpoints
GROUP BY checkpoint_date
ORDER BY checkpoint_date DESC;
```

## Data Quality

### Quality Gates by Layer

#### Bronze Layer
* **Schema Evolution**: Rescue mode captures schema violations in `_rescued_data`
* **Ingestion Tracking**: All rows stamped with `ingested_at` timestamp
* **Error Preservation**: No data loss - bad rows isolated for investigation

#### Silver Layer
* **Expectations**: Constraint-based validation
  * `thread_id IS NOT NULL` (thread lookup)
  * `checkpoint_ts IS NOT NULL` (checkpoints)
* **Action**: `expect_or_drop` - invalid rows excluded from downstream processing
* **CDC Quality**: Auto CDC ensures referential integrity and sequencing

#### Gold Layer
* **Expectations**: Multi-column validation
  * `checkpoint_date IS NOT NULL`
  * `user_id IS NOT NULL` (user metrics)
  * `team IS NOT NULL`
* **Action**: `expect_or_drop` - maintains aggregate accuracy
* **Aggregation Logic**: Tested transformation functions

### Data Quality Monitoring

```sql
-- Expectation violations (requires event log analysis)
-- Check pipeline event logs for dropped rows

-- Null percentages by column
SELECT 
  'silver_checkpoints' AS table_name,
  ROUND(SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS user_id_null_pct,
  ROUND(SUM(CASE WHEN team IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS team_null_pct
FROM main.chat_history.silver_checkpoints;
```

## Performance Optimizations

### Enabled Features

* **Auto-Optimize Writes**: Enabled on Bronze streaming tables
* **Liquid Clustering**: Gold tables clustered by date + team + user dimensions
* **Deletion Vectors**: Efficient UPDATE/DELETE operations
* **Data Skipping**: Indexed columns for predicate pushdown (32 cols)
* **Photon**: Vectorized query engine for accelerated processing
* **Serverless**: Auto-scaling compute with no cluster management

### Maintenance Operations

The pipeline includes maintenance utilities in `utilities/` with automated scheduled jobs:

* **VACUUM**: Removes old file versions (30-day retention)
* **OPTIMIZE**: Compacts small files (automated via auto-optimize)
* **Liquid Clustering Optimization**: Automatic re-clustering on write

#### Scheduled Maintenance Jobs

The following jobs automate routine maintenance tasks:

* [Chatbot Pipeline - Weekly Optimize Maintenance](#job-961719883410776) (Job ID: 961719883410776)
  * **Schedule**: Weekly
  * **Purpose**: Runs OPTIMIZE on all pipeline tables to compact small files
  * **Impact**: Improves query performance and reduces storage costs

* [Chatbot Pipeline - Monthly Vacuum Maintenance](#job-666380740792614) (Job ID: 666380740792614)
  * **Schedule**: Monthly
  * **Purpose**: Runs VACUUM on all pipeline tables to remove old file versions
  * **Retention**: 30 days (configurable in `MaintenanceConfig.VACUUM_RETENTION_HOURS`)
  * **Impact**: Reclaims storage space while preserving time travel capability

#### Manual Maintenance

Run maintenance scripts manually when needed:

```python
# From utilities/maintenance.py
python utilities/maintenance.py --vacuum --tables gold_user_team_metrics,gold_team_summary
```

## Testing

The pipeline includes a comprehensive test suite in `tests/`:

* **Unit Tests**: Test individual transformation functions
* **Integration Tests**: Test full layer pipelines with sample data
* **Fixtures**: Pytest fixtures for SparkSession and sample data

Run tests:

```python
# From pipeline root
python tests/run_tests.py

# Or with pytest directly
pytest tests/ -v
```

## Monitoring

### Pipeline Health

* **Latest Update Status**: Check pipeline monitoring page
* **Event Logs**: Full lineage and execution history
* **Dataset Statistics**: Row counts, update times, data freshness

### Data Profiling

Use queries in `monitoring/data_profiling_queries.sql`:

* Daily record counts by layer
* Processing lag (ingestion → silver → gold)
* Duplicate detection
* Null percentage tracking

### Alerts and Notifications

Configure pipeline notifications for:
* Update failures
* Data quality violations
* Processing lag thresholds

## Best Practices

### Development Workflow

1. **Make Changes**: Edit transformation files in `transformations/`
2. **Dry Run**: Validate syntax and logic without execution
3. **Incremental Update**: Run pipeline on subset of data
4. **Monitor**: Check event logs and data quality
5. **Full Refresh**: Only if schema changes require reprocessing

### Modifying the Pipeline

* **Adding Datasets**: Create in appropriate layer file (bronze/silver/gold)
* **Changing Schemas**: Use schema evolution or add migration script
* **Updating Logic**: Modify functions in `transforms.py`, add unit tests
* **Configuration Changes**: Update `config.py` for centralized management

### Common Pitfalls

* ❌ Don't change dataset types without dropping existing tables
* ❌ Don't mix streaming and batch reads incorrectly
* ❌ Don't skip dry runs before production deployment
* ✅ Always use configuration constants from `config.py`
* ✅ Test transformation functions independently
* ✅ Monitor rescued data for schema violations

## Troubleshooting

### Common Issues

**Pipeline fails with UNRESOLVED_COLUMN**
* Verify upstream table schema with `DESCRIBE TABLE`
* Check Auto Loader `schemaHints` match actual CSV columns
* Ensure CDC target has correct key columns

**No new data processed**
* Check source file paths in Volumes
* Verify Auto Loader checkpoint location
* Review event logs for skipped files

**SCD Type 2 table shows duplicate current records**
* Check `sequence_by` column has proper ordering
* Ensure keys uniquely identify records
* Query with `WHERE __END_AT IS NULL` for current state

**Performance degradation**
* Run OPTIMIZE on Gold tables
* Check for small file problems in Bronze
* Review liquid clustering effectiveness

## Support

* **Pipeline Editor**: Spark Declarative Pipeline UI for code editing
* **Pipeline Monitoring**: Event logs, lineage, and execution history
* **Documentation**: Databricks Spark Declarative Pipelines docs
* **Unity Catalog**: `main.chat_history` catalog for data governance

---

**Pipeline ID**: `3b0f21e6-5a0c-4b1e-bfc9-af9d92905ad4`  
**Owner**: chinhang0104@gmail.com  
**Last Updated**: 2026-05-14
