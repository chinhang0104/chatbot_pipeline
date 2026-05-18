"""
Unit tests for Silver layer transformations.
"""

import pytest
from pyspark.sql import functions as F
from transformations.transforms import clean_thread_lookup, enrich_checkpoints


class TestCleanThreadLookup:
    """Tests for clean_thread_lookup transformation."""
    
    def test_adds_updated_at_column(self, spark, sample_thread_lookup):
        """Test that updated_at timestamp column is added."""
        result = clean_thread_lookup(sample_thread_lookup)
        assert "updated_at" in result.columns
        
    def test_preserves_all_rows(self, spark, sample_thread_lookup):
        """Test that no rows are dropped (no deduplication)."""
        result = clean_thread_lookup(sample_thread_lookup)
        assert result.count() == sample_thread_lookup.count()
        
    def test_output_schema(self, spark, sample_thread_lookup):
        """Test that output has exactly the expected columns."""
        result = clean_thread_lookup(sample_thread_lookup)
        expected_columns = {"thread_id", "user_id", "team", "updated_at"}
        assert set(result.columns) == expected_columns
        
    def test_casts_columns_to_string(self, spark):
        """Test that numeric IDs are cast to string."""
        df = spark.createDataFrame(
            [(1, 100, "team_a")],
            ["thread_id", "user_id", "team"]
        )
        result = clean_thread_lookup(df)
        
        schema_dict = {field.name: str(field.dataType) for field in result.schema.fields}
        assert schema_dict["thread_id"] == "StringType()"
        assert schema_dict["user_id"] == "StringType()"
        assert schema_dict["team"] == "StringType()"
        
    def test_handles_duplicates(self, spark):
        """Test that duplicate rows are preserved (not deduplicated)."""
        df = spark.createDataFrame(
            [("t1", "u1", "team_a"), ("t1", "u1", "team_a")],
            ["thread_id", "user_id", "team"]
        )
        result = clean_thread_lookup(df)
        assert result.count() == 2


class TestEnrichCheckpoints:
    """Tests for enrich_checkpoints transformation."""
    
    def test_parses_json_columns(self, spark, sample_bronze_checkpoints, sample_thread_lookup):
        """Test that JSON columns are parsed correctly."""
        result = enrich_checkpoints(sample_bronze_checkpoints, sample_thread_lookup)
        
        # Check parsed columns exist
        assert "checkpoint_v" in result.columns
        assert "metadata_source" in result.columns
        assert "checkpoint_id_parsed" in result.columns
        
    def test_joins_dimension_table(self, spark, sample_bronze_checkpoints, sample_thread_lookup):
        """Test that dimension table is joined correctly."""
        result = enrich_checkpoints(sample_bronze_checkpoints, sample_thread_lookup)
        
        # Check dimension columns are present
        assert "user_id" in result.columns
        assert "team" in result.columns
        
        # Check a specific join result
        row = result.filter(F.col("thread_id") == "thread_1").first()
        assert row["user_id"] == "user_1"
        assert row["team"] == "team_alpha"
        
    def test_parses_timestamps(self, spark, sample_bronze_checkpoints, sample_thread_lookup):
        """Test that timestamp parsing works correctly."""
        result = enrich_checkpoints(sample_bronze_checkpoints, sample_thread_lookup)
        
        row = result.filter(F.col("checkpoint_id") == "checkpoint_1").first()
        assert row["checkpoint_ts"] is not None
        assert row["checkpoint_date"] is not None
        assert row["checkpoint_hour"] is not None
        
    def test_casts_version_to_int(self, spark, sample_bronze_checkpoints, sample_thread_lookup):
        """Test that checkpoint version is cast to integer."""
        result = enrich_checkpoints(sample_bronze_checkpoints, sample_thread_lookup)
        
        row = result.filter(F.col("checkpoint_id") == "checkpoint_1").first()
        assert row["checkpoint_v"] == 4
        assert isinstance(row["checkpoint_v"], int)
        
    def test_preserves_all_checkpoint_rows(self, spark, sample_bronze_checkpoints, sample_thread_lookup):
        """Test that all checkpoint rows are preserved after join."""
        result = enrich_checkpoints(sample_bronze_checkpoints, sample_thread_lookup)
        assert result.count() == sample_bronze_checkpoints.count()
