from pyspark import pipelines as dp
from pyspark.sql import functions as F
import sys
sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")
from config import MaintenanceConfig, TableNames
from transformations.transforms import (
    gold_user_team_metrics as compute_user_team_metrics,
    gold_team_summary as compute_team_summary
)

# Enhanced table properties for Gold tables
gold_user_team_props = MaintenanceConfig.get_table_properties(TableNames.GOLD_USER_TEAM_METRICS)
gold_user_team_props["quality"] = "gold"

gold_team_summary_props = MaintenanceConfig.get_table_properties(TableNames.GOLD_TEAM_SUMMARY)
gold_team_summary_props["quality"] = "gold"

@dp.materialized_view(
    comment="Gold: checkpoint metrics by user and team",
    table_properties=gold_user_team_props,
    cluster_by=["checkpoint_date", "team", "user_id"]
)
@dp.expect_or_drop("valid_checkpoint_date", "checkpoint_date IS NOT NULL")
@dp.expect_or_drop("valid_user_id", "user_id IS NOT NULL")
@dp.expect_or_drop("valid_team", "team IS NOT NULL")
def gold_user_team_metrics():
    s = spark.read.table("silver_checkpoints")
    return compute_user_team_metrics(s)

@dp.materialized_view(
    comment="Gold: team summary metrics",
    table_properties=gold_team_summary_props,
    cluster_by=["checkpoint_date", "team"]
)
@dp.expect_or_drop("valid_checkpoint_date", "checkpoint_date IS NOT NULL")
@dp.expect_or_drop("valid_team", "team IS NOT NULL")
def gold_team_summary():
    s = spark.read.table("silver_checkpoints")
    return compute_team_summary(s)
