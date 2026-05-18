"""
Pytest configuration and shared fixtures for chatbot pipeline tests.
"""

import pytest
from datetime import datetime
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    Create a Spark session for testing.
    
    In Databricks notebooks, use the existing spark session.
    For local pytest runs, create a new session.
    """
    try:
        # In Databricks, use the existing spark session
        from pyspark.sql import SparkSession
        return SparkSession.getActiveSession()
    except:
        # For local testing
        return (SparkSession.builder
                .master("local[2]")
                .appName("chatbot_pipeline_tests")
                .config("spark.sql.shuffle.partitions", "2")
                .getOrCreate())


@pytest.fixture
def sample_thread_lookup(spark):
    """Sample thread lookup data for testing."""
    return spark.createDataFrame(
        [
            ("thread_1", "user_1", "team_alpha"),
            ("thread_2", "user_2", "team_beta"),
            ("thread_3", "user_1", "team_alpha"),
        ],
        ["thread_id", "user_id", "team"]
    )


@pytest.fixture
def sample_bronze_checkpoints(spark):
    """Sample bronze checkpoint data with JSON columns."""
    return spark.createDataFrame(
        [
            (
                "thread_1",
                1714567890000,
                "checkpoint_1",
                None,
                "checkpoint",
                '{"v":"4","id":"checkpoint_1","ts":"2026-04-01T10:00:00Z","channel_versions":{"__start__":"1"}}',
                '{"step":"1","source":"user_input"}'
            ),
            (
                "thread_1",
                1714567900000,
                "checkpoint_2",
                "checkpoint_1",
                "checkpoint",
                '{"v":"4","id":"checkpoint_2","ts":"2026-04-01T10:05:00Z","channel_versions":{"__start__":"2"}}',
                '{"step":"2","source":"agent_response"}'
            ),
            (
                "thread_2",
                1714654890000,
                "checkpoint_3",
                None,
                "checkpoint",
                '{"v":"4","id":"checkpoint_3","ts":"2026-04-02T09:00:00Z","channel_versions":{"__start__":"1"}}',
                '{"step":"1","source":"user_input"}'
            ),
        ],
        ["thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "type", "checkpoint", "metadata"]
    )


@pytest.fixture
def sample_enriched_checkpoints(spark):
    """Sample enriched checkpoint data for Gold layer testing."""
    return spark.createDataFrame(
        [
            ("thread_1", "user_1", "team_alpha", datetime(2026, 4, 1), datetime(2026, 4, 1, 10, 0), "checkpoint_1", 1),
            ("thread_1", "user_1", "team_alpha", datetime(2026, 4, 1), datetime(2026, 4, 1, 10, 5), "checkpoint_2", 2),
            ("thread_2", "user_2", "team_beta", datetime(2026, 4, 2), datetime(2026, 4, 2, 9, 0), "checkpoint_3", 1),
        ],
        ["thread_id", "user_id", "team", "checkpoint_date", "checkpoint_ts", "checkpoint_id_parsed", "metadata_step_int"]
    )
