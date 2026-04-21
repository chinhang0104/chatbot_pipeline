import dlt
from pyspark.sql import functions as F

@dlt.table(
    comment="Gold: checkpoint metrics by user and team",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid_checkpoint_date", "checkpoint_date IS NOT NULL")
@dlt.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dlt.expect_or_drop("valid_team", "team IS NOT NULL")
def gold_user_team_metrics():
    s = dlt.read("silver_checkpoints")

    return (
        s.groupBy("checkpoint_date", "user_id", "team")
        .agg(
            F.count("*").alias("checkpoint_count"),
            F.countDistinct("thread_id").alias("thread_count"),
            F.countDistinct("checkpoint_id_parsed").alias("unique_checkpoints"),
            F.min("checkpoint_ts").alias("first_checkpoint_ts"),
            F.max("checkpoint_ts").alias("last_checkpoint_ts"),
            F.min("metadata_step_int").alias("min_step"),
            F.max("metadata_step_int").alias("max_step"),
            F.avg("metadata_step_int").alias("avg_step")
        )
    )

@dlt.table(
    comment="Gold: team summary metrics",
    table_properties={"quality": "gold"}
)
@dlt.expect_or_drop("valid_checkpoint_date", "checkpoint_date IS NOT NULL")
@dlt.expect_or_drop("valid_team", "team IS NOT NULL")
def gold_team_summary():
    s = dlt.read("silver_checkpoints")

    return (
        s.groupBy("team", "checkpoint_date")
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