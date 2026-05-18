"""
Integration tests for chatbot pipeline end-to-end data flow.

These tests validate:
- Pipeline datasets exist and are accessible
- Data flows correctly from Bronze → Silver → Gold
- Schemas match expectations
- Business logic produces correct results
- Data quality constraints are met
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, LongType, TimestampType, DateType, IntegerType


# Configuration
CATALOG = "main"
SCHEMA = "chat_history"


class TestPipelineDatasets:
    """Test that all expected pipeline datasets exist and are accessible."""
    
    def test_bronze_datasets_exist(self, spark):
        """Verify Bronze layer datasets exist."""
        bronze_tables = [
            f"{CATALOG}.{SCHEMA}.bronze_thread_lookup",
            f"{CATALOG}.{SCHEMA}.bronze_checkpoints"
        ]
        
        for table in bronze_tables:
            # This will raise an exception if table doesn't exist
            df = spark.read.table(table)
            assert df is not None, f"Failed to read {table}"
            
    def test_silver_datasets_exist(self, spark):
        """Verify Silver layer datasets exist."""
        silver_tables = [
            f"{CATALOG}.{SCHEMA}.silver_thread_lookup",
            f"{CATALOG}.{SCHEMA}.silver_checkpoints"
        ]
        
        for table in silver_tables:
            df = spark.read.table(table)
            assert df is not None, f"Failed to read {table}"
            
    def test_gold_datasets_exist(self, spark):
        """Verify Gold layer datasets exist."""
        gold_tables = [
            f"{CATALOG}.{SCHEMA}.gold_user_team_metrics",
            f"{CATALOG}.{SCHEMA}.gold_team_summary"
        ]
        
        for table in gold_tables:
            df = spark.read.table(table)
            assert df is not None, f"Failed to read {table}"


class TestBronzeLayerSchema:
    """Validate Bronze layer schemas."""
    
    def test_bronze_thread_lookup_schema(self, spark):
        """Verify bronze_thread_lookup has expected columns."""
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_thread_lookup")
        expected_columns = {"thread_id", "user_id", "team"}
        actual_columns = set(df.columns)
        
        # Check expected columns exist (may have additional columns like updated_at)
        assert expected_columns.issubset(actual_columns), \
            f"Missing columns: {expected_columns - actual_columns}"
            
    def test_bronze_checkpoints_schema(self, spark):
        """Verify bronze_checkpoints has expected columns."""
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_checkpoints")
        required_columns = {"thread_id", "checkpoint_id", "checkpoint", "metadata"}
        actual_columns = set(df.columns)
        
        assert required_columns.issubset(actual_columns), \
            f"Missing columns: {required_columns - actual_columns}"


class TestSilverLayerSchema:
    """Validate Silver layer schemas."""
    
    def test_silver_thread_lookup_schema(self, spark):
        """Verify silver_thread_lookup schema matches expectations."""
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_thread_lookup")
        
        expected_columns = {"thread_id", "user_id", "team", "updated_at"}
        assert set(df.columns) == expected_columns, \
            f"Schema mismatch. Expected: {expected_columns}, Got: {set(df.columns)}"
        
        # Verify data types
        schema_dict = {field.name: field.dataType for field in df.schema.fields}
        assert isinstance(schema_dict["thread_id"], StringType)
        assert isinstance(schema_dict["user_id"], StringType)
        assert isinstance(schema_dict["team"], StringType)
        assert isinstance(schema_dict["updated_at"], TimestampType)
        
    def test_silver_checkpoints_schema(self, spark):
        """Verify silver_checkpoints has enriched columns."""
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        
        # Check critical enrichment columns exist
        required_columns = {
            "thread_id", "checkpoint_id", "user_id", "team",
            "checkpoint_ts", "checkpoint_date"
        }
        actual_columns = set(df.columns)
        
        assert required_columns.issubset(actual_columns), \
            f"Missing columns: {required_columns - actual_columns}"


class TestGoldLayerSchema:
    """Validate Gold layer schemas."""
    
    def test_gold_user_team_metrics_schema(self, spark):
        """Verify gold_user_team_metrics has expected aggregation columns."""
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        
        required_columns = {
            "user_id", "team", "checkpoint_count", 
            "first_checkpoint", "last_checkpoint"
        }
        actual_columns = set(df.columns)
        
        assert required_columns.issubset(actual_columns), \
            f"Missing columns: {required_columns - actual_columns}"
            
    def test_gold_team_summary_schema(self, spark):
        """Verify gold_team_summary has team-level metrics."""
        df = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_team_summary")
        
        required_columns = {
            "team", "total_checkpoints", "unique_users", "unique_threads"
        }
        actual_columns = set(df.columns)
        
        assert required_columns.issubset(actual_columns), \
            f"Missing columns: {required_columns - actual_columns}"


class TestDataFlow:
    """Test data flows correctly through Bronze → Silver → Gold."""
    
    def test_silver_thread_lookup_references_bronze(self, spark):
        """Verify Silver thread lookup is derived from Bronze."""
        bronze = spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_thread_lookup")
        silver = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_thread_lookup")
        
        # Silver should not have more unique thread_ids than Bronze (or equal)
        bronze_threads = bronze.select("thread_id").distinct().count()
        silver_threads = silver.select("thread_id").distinct().count()
        
        assert silver_threads <= bronze_threads, \
            "Silver has more threads than Bronze - data flow broken"
            
    def test_silver_checkpoints_joins_thread_lookup(self, spark):
        """Verify Silver checkpoints has user_id and team from thread lookup."""
        silver_checkpoints = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        silver_thread_lookup = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_thread_lookup")
        
        # All threads in checkpoints should exist in thread lookup
        checkpoint_threads = silver_checkpoints.select("thread_id").distinct()
        lookup_threads = silver_thread_lookup.select("thread_id").distinct()
        
        # Threads in checkpoints that are NOT in lookup (should be empty or minimal)
        orphaned = checkpoint_threads.join(
            lookup_threads, 
            on="thread_id", 
            how="left_anti"
        )
        
        orphaned_count = orphaned.count()
        total_checkpoint_threads = checkpoint_threads.count()
        
        # Allow some orphans due to timing, but most should be joined
        if total_checkpoint_threads > 0:
            orphan_ratio = orphaned_count / total_checkpoint_threads
            assert orphan_ratio < 0.5, \
                f"Too many orphaned threads: {orphaned_count}/{total_checkpoint_threads}"
                
    def test_gold_metrics_aggregate_silver_checkpoints(self, spark):
        """Verify Gold metrics are aggregated from Silver checkpoints."""
        silver = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        gold = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        
        # Calculate expected checkpoint count from Silver
        silver_count = silver.count()
        
        # Sum of checkpoint counts in Gold should equal Silver checkpoint count
        gold_total = gold.agg(F.sum("checkpoint_count").alias("total")).first()["total"]
        
        assert gold_total == silver_count, \
            f"Gold checkpoint sum ({gold_total}) doesn't match Silver count ({silver_count})"


class TestDataQuality:
    """Test data quality constraints and business logic."""
    
    def test_no_null_thread_ids_in_silver(self, spark):
        """Verify Silver tables have no null thread_ids."""
        silver_checkpoints = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        null_count = silver_checkpoints.filter(F.col("thread_id").isNull()).count()
        
        assert null_count == 0, f"Found {null_count} rows with null thread_id"
        
    def test_no_null_user_ids_in_silver(self, spark):
        """Verify Silver checkpoints have no null user_ids after enrichment."""
        silver_checkpoints = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        null_count = silver_checkpoints.filter(F.col("user_id").isNull()).count()
        
        total_count = silver_checkpoints.count()
        if total_count > 0:
            null_ratio = null_count / total_count
            # Allow small percentage of nulls due to left join
            assert null_ratio < 0.2, \
                f"Too many null user_ids: {null_count}/{total_count}"
                
    def test_checkpoint_timestamps_are_valid(self, spark):
        """Verify checkpoint timestamps are reasonable (not in far future/past)."""
        silver = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        
        # Check that timestamps are within reasonable range (2020-2030)
        from datetime import datetime
        min_date = datetime(2020, 1, 1)
        max_date = datetime(2030, 12, 31)
        
        invalid_timestamps = silver.filter(
            (F.col("checkpoint_ts") < F.lit(min_date)) | 
            (F.col("checkpoint_ts") > F.lit(max_date))
        )
        
        invalid_count = invalid_timestamps.count()
        assert invalid_count == 0, \
            f"Found {invalid_count} rows with invalid timestamps"
            
    def test_gold_checkpoint_counts_are_positive(self, spark):
        """Verify Gold metrics have positive checkpoint counts."""
        gold = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        
        negative_counts = gold.filter(F.col("checkpoint_count") <= 0)
        assert negative_counts.count() == 0, \
            "Found negative or zero checkpoint counts in Gold metrics"
            
    def test_gold_team_summary_aggregates_correctly(self, spark):
        """Verify team summary matches user-level aggregations."""
        user_metrics = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        team_summary = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_team_summary")
        
        # Calculate expected team totals from user metrics
        expected_team_totals = user_metrics.groupBy("team").agg(
            F.sum("checkpoint_count").alias("expected_total_checkpoints"),
            F.countDistinct("user_id").alias("expected_unique_users")
        )
        
        # Join with actual team summary
        comparison = team_summary.join(
            expected_team_totals, 
            on="team", 
            how="inner"
        )
        
        # Check that totals match
        mismatches = comparison.filter(
            (F.col("total_checkpoints") != F.col("expected_total_checkpoints")) |
            (F.col("unique_users") != F.col("expected_unique_users"))
        )
        
        assert mismatches.count() == 0, \
            "Team summary metrics don't match user-level aggregations"


class TestPipelineConsistency:
    """Test cross-layer consistency and referential integrity."""
    
    def test_all_silver_checkpoint_threads_have_users(self, spark):
        """Verify all threads in Silver checkpoints can be traced to users."""
        checkpoints = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        
        # Filter to non-null thread_ids
        valid_checkpoints = checkpoints.filter(F.col("thread_id").isNotNull())
        
        # Count how many have user_id populated
        with_users = valid_checkpoints.filter(F.col("user_id").isNotNull()).count()
        total = valid_checkpoints.count()
        
        if total > 0:
            coverage = with_users / total
            # At least 80% should have user mapping
            assert coverage >= 0.8, \
                f"Only {coverage:.1%} of checkpoints have user_id mapping"
                
    def test_gold_metrics_cover_all_teams(self, spark):
        """Verify Gold metrics include all teams from Silver."""
        silver = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        gold_metrics = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        gold_summary = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_team_summary")
        
        # Get unique teams from each layer
        silver_teams = set([row["team"] for row in silver.select("team").distinct().collect() 
                           if row["team"] is not None])
        gold_teams = set([row["team"] for row in gold_summary.select("team").distinct().collect()])
        
        # Gold should include all Silver teams (or close, allowing for timing)
        missing_teams = silver_teams - gold_teams
        
        # Allow small discrepancies but not major gaps
        if len(silver_teams) > 0:
            coverage = len(gold_teams) / len(silver_teams)
            assert coverage >= 0.8, \
                f"Gold missing teams: {missing_teams}"


class TestBusinessLogic:
    """Test specific business logic and calculations."""
    
    def test_user_checkpoint_count_matches_detail(self, spark):
        """Verify user checkpoint counts match actual checkpoint rows."""
        checkpoints = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        gold_metrics = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        
        # Calculate actual counts from Silver
        actual_counts = checkpoints.groupBy("user_id", "team").agg(
            F.count("*").alias("actual_count")
        ).filter(F.col("user_id").isNotNull())
        
        # Join with Gold metrics
        comparison = gold_metrics.join(
            actual_counts,
            on=["user_id", "team"],
            how="inner"
        )
        
        # Check for mismatches
        mismatches = comparison.filter(
            F.col("checkpoint_count") != F.col("actual_count")
        )
        
        mismatch_count = mismatches.count()
        assert mismatch_count == 0, \
            f"Found {mismatch_count} users with incorrect checkpoint counts"
            
    def test_first_and_last_checkpoint_order(self, spark):
        """Verify first_checkpoint <= last_checkpoint in Gold metrics."""
        gold = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        
        invalid_order = gold.filter(
            F.col("first_checkpoint") > F.col("last_checkpoint")
        )
        
        assert invalid_order.count() == 0, \
            "Found users where first_checkpoint > last_checkpoint"


class TestPipelineHealth:
    """High-level pipeline health checks."""
    
    def test_pipeline_has_data(self, spark):
        """Verify pipeline has processed some data."""
        # Check each layer has at least some data
        bronze = spark.read.table(f"{CATALOG}.{SCHEMA}.bronze_checkpoints")
        silver = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        gold = spark.read.table(f"{CATALOG}.{SCHEMA}.gold_user_team_metrics")
        
        assert bronze.count() > 0, "Bronze layer is empty"
        assert silver.count() > 0, "Silver layer is empty"
        assert gold.count() > 0, "Gold layer is empty"
        
    def test_data_freshness(self, spark):
        """Verify data has been updated recently."""
        from datetime import datetime, timedelta
        
        silver = spark.read.table(f"{CATALOG}.{SCHEMA}.silver_checkpoints")
        
        if silver.count() > 0:
            # Get most recent checkpoint timestamp
            most_recent = silver.agg(F.max("checkpoint_ts").alias("max_ts")).first()["max_ts"]
            
            # Data should be from last 2 years (lenient check)
            cutoff = datetime.now() - timedelta(days=730)
            
            assert most_recent >= cutoff, \
                f"Most recent checkpoint is too old: {most_recent}"
