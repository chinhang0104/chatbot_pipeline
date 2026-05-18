-- ============================================================================
-- DATA PROFILING MONITORING TABLES
-- Automated monitoring materialized views for chatbot_pipeline
-- These tables refresh automatically when the pipeline runs
-- ============================================================================

-- ============================================================================
-- 1. DAILY RECORD COUNTS BY LAYER
-- Purpose: Track record volume trends across bronze, silver, and gold layers
--          to identify ingestion anomalies or processing drops
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW monitoring_daily_counts
COMMENT 'Daily record counts by pipeline layer for volume monitoring'
AS
SELECT 
  'bronze_checkpoints' AS layer,
  DATE(ingested_at) AS date,
  COUNT(*) AS record_count
FROM main.chat_history.bronze_checkpoints
GROUP BY DATE(ingested_at)

UNION ALL

SELECT 
  'silver_thread_lookup_cdc' AS layer,
  DATE(updated_at) AS date,
  COUNT(*) AS record_count
FROM main.chat_history.silver_thread_lookup_cdc
GROUP BY DATE(updated_at)

UNION ALL

SELECT 
  'silver_checkpoints' AS layer,
  checkpoint_date AS date,
  COUNT(*) AS record_count
FROM main.chat_history.silver_checkpoints
GROUP BY checkpoint_date

UNION ALL

SELECT 
  'gold_user_team_metrics' AS layer,
  checkpoint_date AS date,
  COUNT(*) AS record_count
FROM main.chat_history.gold_user_team_metrics
GROUP BY checkpoint_date

UNION ALL

SELECT 
  'gold_team_summary' AS layer,
  checkpoint_date AS date,
  COUNT(*) AS record_count
FROM main.chat_history.gold_team_summary
GROUP BY checkpoint_date;


-- ============================================================================
-- 2. NULL VALUE PERCENTAGES
-- Purpose: Monitor data quality by tracking NULL percentages for key columns
--          Helps identify data completeness issues early in the pipeline
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW monitoring_null_percentages
COMMENT 'NULL percentage tracking for key columns across all layers'
AS
-- Bronze Layer: Raw checkpoints NULL analysis
SELECT 
  'bronze_checkpoints' AS table_name,
  'thread_id' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN thread_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.bronze_checkpoints

UNION ALL

SELECT 
  'bronze_checkpoints' AS table_name,
  'checkpoint_id' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN checkpoint_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.bronze_checkpoints

UNION ALL

SELECT 
  'bronze_checkpoints' AS table_name,
  'checkpoint_ns' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN checkpoint_ns IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.bronze_checkpoints

UNION ALL

SELECT 
  'bronze_checkpoints' AS table_name,
  'type' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN type IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.bronze_checkpoints

UNION ALL

SELECT 
  'bronze_checkpoints' AS table_name,
  'metadata' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN metadata IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.bronze_checkpoints

UNION ALL

-- Silver Layer: Thread lookup NULL analysis
SELECT 
  'silver_thread_lookup_cdc' AS table_name,
  'thread_id' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN thread_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_thread_lookup_cdc

UNION ALL

SELECT 
  'silver_thread_lookup_cdc' AS table_name,
  'user_id' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_thread_lookup_cdc

UNION ALL

SELECT 
  'silver_thread_lookup_cdc' AS table_name,
  'team' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN team IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_thread_lookup_cdc

UNION ALL

-- Silver Layer: Checkpoints enriched NULL analysis
SELECT 
  'silver_checkpoints' AS table_name,
  'checkpoint_ts' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN checkpoint_ts IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_checkpoints

UNION ALL

SELECT 
  'silver_checkpoints' AS table_name,
  'user_id' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_checkpoints

UNION ALL

SELECT 
  'silver_checkpoints' AS table_name,
  'team' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN team IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_checkpoints

UNION ALL

SELECT 
  'silver_checkpoints' AS table_name,
  'metadata_step_int' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN metadata_step_int IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_checkpoints

UNION ALL

SELECT 
  'silver_checkpoints' AS table_name,
  'metadata_source' AS column_name,
  ROUND(100.0 * SUM(CASE WHEN metadata_source IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS null_percentage,
  COUNT(*) AS total_records,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_checkpoints;


-- ============================================================================
-- 3. DUPLICATE DETECTION
-- Purpose: Identify duplicate records that violate expected uniqueness constraints
--          Critical for ensuring data integrity across pipeline layers
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW monitoring_duplicates
COMMENT 'Duplicate records detection across bronze and silver layers'
AS
-- Duplicates in bronze_checkpoints (by thread_id + checkpoint_ns + checkpoint_id)
SELECT 
  'bronze_checkpoints' AS table_name,
  thread_id,
  checkpoint_ns,
  checkpoint_id,
  NULL AS user_ids,
  NULL AS teams,
  COUNT(*) AS duplicate_count,
  MIN(ingested_at) AS first_seen,
  MAX(ingested_at) AS last_seen,
  CURRENT_TIMESTAMP() AS detected_at
FROM main.chat_history.bronze_checkpoints
GROUP BY thread_id, checkpoint_ns, checkpoint_id
HAVING COUNT(*) > 1

UNION ALL

-- Duplicates in silver_thread_lookup_cdc (by thread_id)
SELECT 
  'silver_thread_lookup_cdc' AS table_name,
  thread_id,
  NULL AS checkpoint_ns,
  NULL AS checkpoint_id,
  CONCAT('[', CONCAT_WS(',', COLLECT_LIST(CONCAT('"', user_id, '"'))), ']') AS user_ids,
  CONCAT('[', CONCAT_WS(',', COLLECT_LIST(CONCAT('"', team, '"'))), ']') AS teams,
  COUNT(*) AS duplicate_count,
  MIN(updated_at) AS first_seen,
  MAX(updated_at) AS last_seen,
  CURRENT_TIMESTAMP() AS detected_at
FROM main.chat_history.silver_thread_lookup_cdc
GROUP BY thread_id
HAVING COUNT(*) > 1

UNION ALL

-- Duplicates in silver_checkpoints (by thread_id + checkpoint_id)
SELECT 
  'silver_checkpoints' AS table_name,
  thread_id,
  checkpoint_ns,
  checkpoint_id,
  CONCAT('[', CONCAT_WS(',', COLLECT_LIST(DISTINCT CONCAT('"', user_id, '"'))), ']') AS user_ids,
  CONCAT('[', CONCAT_WS(',', COLLECT_LIST(DISTINCT CONCAT('"', team, '"'))), ']') AS teams,
  COUNT(*) AS duplicate_count,
  MIN(checkpoint_ts) AS first_seen,
  MAX(checkpoint_ts) AS last_seen,
  CURRENT_TIMESTAMP() AS detected_at
FROM main.chat_history.silver_checkpoints
GROUP BY thread_id, checkpoint_ns, checkpoint_id
HAVING COUNT(*) > 1;


-- ============================================================================
-- 4. PROCESSING LAG METRICS
-- Purpose: Monitor time delays between event occurrence and ingestion
--          Helps detect pipeline bottlenecks and processing delays
-- ============================================================================

CREATE OR REFRESH MATERIALIZED VIEW monitoring_processing_lag
COMMENT 'Processing lag metrics by date (event time vs ingestion time)'
AS
SELECT 
  DATE(checkpoint_ts) AS event_date,
  COUNT(*) AS record_count,
  ROUND(AVG(UNIX_TIMESTAMP(b.ingested_at) - UNIX_TIMESTAMP(s.checkpoint_ts)) / 60, 2) AS avg_lag_minutes,
  ROUND(MIN(UNIX_TIMESTAMP(b.ingested_at) - UNIX_TIMESTAMP(s.checkpoint_ts)) / 60, 2) AS min_lag_minutes,
  ROUND(MAX(UNIX_TIMESTAMP(b.ingested_at) - UNIX_TIMESTAMP(s.checkpoint_ts)) / 60, 2) AS max_lag_minutes,
  ROUND(PERCENTILE_APPROX((UNIX_TIMESTAMP(b.ingested_at) - UNIX_TIMESTAMP(s.checkpoint_ts)) / 60, 0.50), 2) AS median_lag_minutes,
  ROUND(PERCENTILE_APPROX((UNIX_TIMESTAMP(b.ingested_at) - UNIX_TIMESTAMP(s.checkpoint_ts)) / 60, 0.95), 2) AS p95_lag_minutes,
  CURRENT_TIMESTAMP() AS measured_at
FROM main.chat_history.silver_checkpoints s
JOIN main.chat_history.bronze_checkpoints b 
  ON s.thread_id = b.thread_id 
  AND s.checkpoint_ns = b.checkpoint_ns 
  AND s.checkpoint_id = b.checkpoint_id
WHERE s.checkpoint_ts IS NOT NULL
GROUP BY DATE(checkpoint_ts);
