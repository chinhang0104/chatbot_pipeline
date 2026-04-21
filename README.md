# Chatbot Pipeline

A Spark Declarative Pipeline (SDP) for processing chatbot checkpoint data using the Medallion architecture.

## Overview

This pipeline ingests, cleanses, and aggregates chatbot checkpoint data to produce user and team analytics. It follows the Bronze → Silver → Gold pattern for incremental data processing.

## Architecture

### Data Flow

```
Raw CSV Files (Volumes) 
    → Bronze (Auto Loader ingestion)
    → Silver (Data cleansing & enrichment)  
    → Gold (Aggregated metrics)
```

### Medallion Layers

* **Bronze Layer**: Raw data ingestion with Auto Loader
* **Silver Layer**: Cleaned, enriched data with user/team attribution
* **Gold Layer**: Business-level aggregations and metrics

## Data Sources

* **Checkpoints CSV**: `/Volumes/main/chat_history/raw_data/checkpoints_*.csv`
  * Incremental ingestion using Auto Loader
  * Contains checkpoint events with JSON-encoded metadata
  
* **Thread Lookup CSV**: `/Volumes/main/chat_history/raw_data/thread_lookup.csv`
  * Dimension table mapping thread_id → user_id + team
  * Uses SCD Type 1 (overwrites on change)
  * Change detection via file modification time

## Datasets

### Bronze Layer (`transformations/bronze.py`)

#### `bronze_checkpoints`
* **Type**: Streaming Table
* **Source**: Auto Loader (cloudFiles)
* **Schema Evolution**: Rescue mode with `_rescued_data` column
* **Features**:
  * Incremental CSV ingestion
  * Auto-optimized writes
  * Captures ingestion timestamp

### Silver Layer (`transformations/silver.py`)

#### `silver_thread_lookup`
* **Type**: Materialized View (SCD Type 1)
* **Source**: Static CSV with change detection
* **Data Quality**: Drops rows where `thread_id IS NULL`
* **Features**:
  * Skips processing if lookup file unchanged
  * Tracks file modification time via table properties
  * Overwrites existing records (upserts)

#### `silver_checkpoints`
* **Type**: Materialized View
* **Source**: `bronze_checkpoints` joined with `silver_thread_lookup`
* **Data Quality**: Drops rows where `checkpoint_ts IS NULL`
* **Features**:
  * Parses JSON checkpoint and metadata fields
  * Extracts timestamp, version, step, source
  * Enriches with user_id and team
  * Creates derived time columns (date, hour)

### Gold Layer (`transformations/gold.py`)

#### `gold_user_team_metrics`
* **Type**: Materialized View
* **Granularity**: Per user, per team, per day
* **Metrics**:
  * Checkpoint counts (total, unique, per thread)
  * First/last checkpoint timestamps
  * Min/max/avg step numbers

#### `gold_team_summary`
* **Type**: Materialized View
* **Granularity**: Per team, per day
* **Metrics**:
  * Checkpoint counts (total, unique)
  * User count (distinct users per team)
  * Thread count (distinct threads)
  * Step statistics

## Directory Structure

```
chatbot_pipeline/
├── transformations/
│   ├── bronze.py      # Data ingestion
│   ├── silver.py      # Data cleansing & enrichment
│   └── gold.py        # Aggregated metrics
└── utilities/
    ├── maintenance.py  # Pipeline maintenance tasks
    ├── history.sql     # Historical queries
    └── restore.sql     # Data restoration scripts
```

## Usage

### Running the Pipeline

1. **Full Refresh** (reprocess all data):
   ```
   Start pipeline update with full refresh
   ```

2. **Incremental Update** (process new data only):
   ```
   Start pipeline update
   ```

3. **Dry Run** (validate without executing):
   ```
   Start pipeline dry run
   ```

### Querying the Data

```sql
-- User metrics by date
SELECT * FROM main.chat_history.gold_user_team_metrics
WHERE checkpoint_date = CURRENT_DATE()
ORDER BY checkpoint_count DESC;

-- Team summary
SELECT * FROM main.chat_history.gold_team_summary
WHERE checkpoint_date >= CURRENT_DATE() - INTERVAL 7 DAYS
ORDER BY team, checkpoint_date;
```

## Data Quality

* **Bronze**: Rescue mode captures schema violations in `_rescued_data`
* **Silver**: Expectations drop invalid rows (null timestamps, null thread IDs)
* **Gold**: Aggregations over validated, cleaned data

## Performance Optimizations

* Auto-optimized writes enabled on Bronze tables
* Incremental processing via Auto Loader
* Change detection on dimension table to skip unnecessary processing
* Left join pattern for optional enrichment

## Monitoring

* **Auto Loader schema location**: `/Volumes/main/chat_history/_checkpoints_schema`
* **Lookup file tracking**: Table property `lookup_file_mtime` on `silver_thread_lookup`

## Notes

* Thread lookup uses SCD Type 1 (latest version overwrites)
* Checkpoint data is append-only (no updates/deletes)
* All timestamps are in UTC
* JSON parsing handles nested structures in checkpoint metadata
