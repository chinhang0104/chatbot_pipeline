"""
Unit tests for Gold layer aggregation transformations.
"""

import pytest
from pyspark.sql import functions as F
from transformations.transforms import gold_user_team_metrics, gold_team_summary


class TestGoldUserTeamMetrics:
    """Tests for gold_user_team_metrics aggregation."""
    
    def test_groups_by_date_user_team(self, spark, sample_enriched_checkpoints):
        """Test that aggregation groups by checkpoint_date, user_id, and team."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        # Should have 2 groups: (2026-04-01, user_1, team_alpha) and (2026-04-02, user_2, team_beta)
        assert result.count() == 2
        
    def test_calculates_checkpoint_count(self, spark, sample_enriched_checkpoints):
        """Test that checkpoint_count is calculated correctly."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        # user_1 on 2026-04-01 should have 2 checkpoints
        row = result.filter(
            (F.col("user_id") == "user_1") & 
            (F.col("checkpoint_date") == "2026-04-01")
        ).first()
        
        assert row["checkpoint_count"] == 2
        
    def test_calculates_thread_count(self, spark, sample_enriched_checkpoints):
        """Test that distinct thread_count is calculated correctly."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        row = result.filter(F.col("user_id") == "user_1").first()
        assert row["thread_count"] == 1  # user_1 has only thread_1
        
    def test_calculates_unique_checkpoints(self, spark, sample_enriched_checkpoints):
        """Test that unique checkpoint count is correct."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        row = result.filter(F.col("user_id") == "user_1").first()
        assert row["unique_checkpoints"] == 2
        
    def test_calculates_step_statistics(self, spark, sample_enriched_checkpoints):
        """Test that min, max, and avg step are calculated correctly."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        row = result.filter(F.col("user_id") == "user_1").first()
        assert row["min_step"] == 1
        assert row["max_step"] == 2
        assert row["avg_step"] == 1.5
        
    def test_calculates_timestamp_range(self, spark, sample_enriched_checkpoints):
        """Test that first and last checkpoint timestamps are captured."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        row = result.filter(F.col("user_id") == "user_1").first()
        assert row["first_checkpoint_ts"] is not None
        assert row["last_checkpoint_ts"] is not None
        assert row["first_checkpoint_ts"] <= row["last_checkpoint_ts"]
        
    def test_output_schema(self, spark, sample_enriched_checkpoints):
        """Test that output has all expected columns."""
        result = gold_user_team_metrics(sample_enriched_checkpoints)
        
        expected_columns = {
            "checkpoint_date", "user_id", "team",
            "checkpoint_count", "thread_count", "unique_checkpoints",
            "first_checkpoint_ts", "last_checkpoint_ts",
            "min_step", "max_step", "avg_step"
        }
        assert set(result.columns) == expected_columns


class TestGoldTeamSummary:
    """Tests for gold_team_summary aggregation."""
    
    def test_groups_by_team_and_date(self, spark, sample_enriched_checkpoints):
        """Test that aggregation groups by team and checkpoint_date."""
        result = gold_team_summary(sample_enriched_checkpoints)
        
        # Should have 2 groups: (team_alpha, 2026-04-01) and (team_beta, 2026-04-02)
        assert result.count() == 2
        
    def test_calculates_checkpoint_count(self, spark, sample_enriched_checkpoints):
        """Test that team checkpoint count is calculated correctly."""
        result = gold_team_summary(sample_enriched_checkpoints)
        
        row = result.filter(F.col("team") == "team_alpha").first()
        assert row["checkpoint_count"] == 2
        
    def test_calculates_user_count(self, spark, sample_enriched_checkpoints):
        """Test that distinct user count is calculated correctly."""
        result = gold_team_summary(sample_enriched_checkpoints)
        
        row = result.filter(F.col("team") == "team_alpha").first()
        assert row["user_count"] == 1  # Only user_1 in team_alpha
        
    def test_calculates_thread_count(self, spark, sample_enriched_checkpoints):
        """Test that distinct thread count is calculated correctly."""
        result = gold_team_summary(sample_enriched_checkpoints)
        
        row = result.filter(F.col("team") == "team_alpha").first()
        assert row["thread_count"] == 1  # Only thread_1 for team_alpha
        
    def test_output_schema(self, spark, sample_enriched_checkpoints):
        """Test that output has all expected columns."""
        result = gold_team_summary(sample_enriched_checkpoints)
        
        expected_columns = {
            "team", "checkpoint_date",
            "checkpoint_count", "user_count", "thread_count", "unique_checkpoints",
            "first_checkpoint_ts", "last_checkpoint_ts",
            "min_step", "max_step", "avg_step"
        }
        assert set(result.columns) == expected_columns
