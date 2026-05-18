"""
Transformation functions for Bronze, Silver, and Gold layers.

These functions are pure transformations that take DataFrames as input
and return transformed DataFrames. They are used by the pipeline datasets.
"""

import sys
sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from config import SchemaConfig


def clean_thread_lookup(df):
    """
    Clean and standardize thread lookup data.
    
    Casts all columns to string and adds an updated_at timestamp.
    """
    return (
        df.select(
            F.col("thread_id").cast("string").alias("thread_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("team").cast("string").alias("team"),
            F.current_timestamp().alias("updated_at")
        )
    )


def enrich_checkpoints(b, dim):
    """
    Enrich checkpoint data with parsed JSON fields and dimension table.
    
    Args:
        b: Bronze checkpoint DataFrame with JSON columns
        dim: Dimension table with thread_id -> user_id/team mapping
        
    Returns:
        Enriched DataFrame with parsed fields and dimension attributes
    """
    return (
        b
        .withColumn("checkpoint_parsed", F.from_json(F.col("checkpoint"), SchemaConfig.CHECKPOINT_SCHEMA))
        .withColumn("metadata_parsed", F.from_json(F.col("metadata"), SchemaConfig.METADATA_SCHEMA))
        .withColumn("checkpoint_ts", F.to_timestamp("checkpoint_parsed.ts"))
        .withColumn("checkpoint_date", F.to_date("checkpoint_ts"))
        .withColumn("checkpoint_hour", F.date_trunc("hour", "checkpoint_ts"))
        .withColumn("checkpoint_v", F.col("checkpoint_parsed.v").cast(IntegerType()))
        .withColumn("checkpoint_id_parsed", F.col("checkpoint_parsed.id"))
        .withColumn("metadata_step_int", F.col("metadata_parsed.step").cast(IntegerType()))
        .withColumn("metadata_source", F.col("metadata_parsed.source"))
        .withColumn("channel_versions_start", F.col("checkpoint_parsed.channel_versions")["__start__"])
        .select(
            "thread_id",
            "checkpoint_ns",
            "checkpoint_id",
            "parent_checkpoint_id",
            "type",
            "checkpoint_ts",
            "checkpoint_date",
            "checkpoint_hour",
            "checkpoint_v",
            "checkpoint_id_parsed",
            "metadata_step_int",
            "metadata_source",
            "channel_versions_start"
        )
        .join(dim, on="thread_id", how="left")
    )


def gold_user_team_metrics(df):
    """
    Calculate user and team metrics aggregated by date.
    
    Returns daily metrics per user/team combination including
    checkpoint counts, thread counts, and step statistics.
    """
    return (
        df.groupBy("checkpoint_date", "user_id", "team")
          .agg(
              F.count("*").alias("checkpoint_count"),
              F.countDistinct("thread_id").alias("thread_count"),
              F.countDistinct("checkpoint_id_parsed").alias("unique_checkpoints"),
              F.min("checkpoint_ts").alias("first_checkpoint_ts"),
              F.max("checkpoint_ts").alias("last_checkpoint_ts"),
              F.min("metadata_step_int").alias("min_step"),
              F.max("metadata_step_int").alias("max_step"),
              F.avg("metadata_step_int").alias("avg_step"),
          )
    )


def gold_team_summary(df):
    """
    Calculate team-level summary metrics aggregated by date.
    
    Returns daily metrics per team including checkpoint counts,
    user counts, thread counts, and step statistics.
    """
    return (
        df.groupBy("team", "checkpoint_date")
          .agg(
              F.count("*").alias("checkpoint_count"),
              F.countDistinct("user_id").alias("user_count"),
              F.countDistinct("thread_id").alias("thread_count"),
              F.countDistinct("checkpoint_id_parsed").alias("unique_checkpoints"),
              F.min("checkpoint_ts").alias("first_checkpoint_ts"),
              F.max("checkpoint_ts").alias("last_checkpoint_ts"),
              F.min("metadata_step_int").alias("min_step"),
              F.max("metadata_step_int").alias("max_step"),
              F.avg("metadata_step_int").alias("avg_step")
          )
    )
