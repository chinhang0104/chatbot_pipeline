# Databricks notebook source
# DBTITLE 1,Cell 1
import sys
sys.path.append("/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")
from pyspark.sql import functions as F

# COMMAND ----------

## Silver

# COMMAND ----------

# DBTITLE 1,Cell 3
from pyspark.sql import functions as F
from transformations.transforms import clean_thread_lookup, enrich_checkpoints

def test_clean_thread_lookup(spark):
    df = spark.createDataFrame(
        [("t1", "u1", "team_a"), ("t1", "u1", "team_a"), ("t2", "u2", "team_b")],
        ["thread_id", "user_id", "team"]
    )
    out = clean_thread_lookup(df)
    assert out.count() == 3  # No deduplication in function
    assert set(out.columns) == {"thread_id", "user_id", "team", "updated_at"}  # includes updated_at
    print("test_clean_thread_lookup PASSED")

def test_enrich_checkpoints(spark):
    # Create test data with all required columns
    cp = spark.createDataFrame(
        [("t1", 1234567890, "c1", "p1", "type1", '{"v":"4","id":"c1","ts":"2026-04-01T10:00:00Z","channel_versions":{"__start__":"1"}}', '{"step":"1","source":"input"}')],
        ["thread_id", "checkpoint_ns", "checkpoint_id", "parent_checkpoint_id", "type", "checkpoint", "metadata"]
    )

    dim = spark.createDataFrame([("t1", "u1", "team_a")], ["thread_id", "user_id", "team"])
    out = enrich_checkpoints(cp, dim)
    
    assert out.count() == 1
    r = out.collect()[0]
    assert r["user_id"] == "u1"
    assert r["team"] == "team_a"
    assert r["checkpoint_v"] == 4
    assert r["metadata_source"] == "input"
    print("test_enrich_checkpoints PASSED")

# COMMAND ----------

# DBTITLE 1,Cell 4
print("=" * 50)
print("RUNNING SILVER LAYER TESTS")
print("=" * 50)

test_clean_thread_lookup(spark)
test_enrich_checkpoints(spark)

print("\n" + "=" * 50)
print("✓ ALL SILVER LAYER TESTS PASSED!")
print("=" * 50)

# COMMAND ----------

## Gold

# COMMAND ----------

# DBTITLE 1,Cell 6
from transformations.transforms import gold_user_team_metrics, gold_team_summary
from datetime import datetime

def test_gold_user_team_metrics(spark):
    # Create enriched checkpoints data
    test_data = [
        ("t1", "u1", "team_a", datetime(2026, 4, 1), datetime(2026, 4, 1, 10, 0), "c1", 1),
        ("t1", "u1", "team_a", datetime(2026, 4, 1), datetime(2026, 4, 1, 11, 0), "c2", 2),
        ("t2", "u2", "team_b", datetime(2026, 4, 2), datetime(2026, 4, 2, 9, 0), "c3", 1)
    ]
    df = spark.createDataFrame(
        test_data,
        ["thread_id", "user_id", "team", "checkpoint_date", "checkpoint_ts", "checkpoint_id_parsed", "metadata_step_int"]
    )
    
    out = gold_user_team_metrics(df)
    assert out.count() == 2  # Two unique (date, user, team) combinations
    
    # Check for team_a user
    team_a_row = out.filter((F.col("user_id") == "u1") & (F.col("team") == "team_a")).collect()[0]
    assert team_a_row["checkpoint_count"] == 2
    assert team_a_row["thread_count"] == 1
    assert team_a_row["unique_checkpoints"] == 2
    assert team_a_row["min_step"] == 1
    assert team_a_row["max_step"] == 2
    
    print("test_gold_user_team_metrics PASSED")

def test_gold_team_summary(spark):
    # Create enriched checkpoints data
    test_data = [
        ("t1", "u1", "team_a", datetime(2026, 4, 1), datetime(2026, 4, 1, 10, 0), "c1", 1),
        ("t1", "u2", "team_a", datetime(2026, 4, 1), datetime(2026, 4, 1, 11, 0), "c2", 2),
        ("t2", "u3", "team_b", datetime(2026, 4, 2), datetime(2026, 4, 2, 9, 0), "c3", 1)
    ]
    df = spark.createDataFrame(
        test_data,
        ["thread_id", "user_id", "team", "checkpoint_date", "checkpoint_ts", "checkpoint_id_parsed", "metadata_step_int"]
    )
    
    out = gold_team_summary(df)
    assert out.count() == 2  # Two unique (team, date) combinations
    
    # Check for team_a summary
    team_a_row = out.filter(F.col("team") == "team_a").collect()[0]
    assert team_a_row["checkpoint_count"] == 2
    assert team_a_row["user_count"] == 2
    assert team_a_row["thread_count"] == 1
    assert team_a_row["unique_checkpoints"] == 2
    assert team_a_row["min_step"] == 1
    assert team_a_row["max_step"] == 2
    
    print("test_gold_team_summary PASSED")

# COMMAND ----------

# DBTITLE 1,Cell 6
print("=" * 50)
print("RUNNING GOLD LAYER TESTS")
print("=" * 50)

test_gold_user_team_metrics(spark)
test_gold_team_summary(spark)

print("\n" + "=" * 50)
print("✓ ALL GOLD LAYER TESTS PASSED!")
print("=" * 50)

# COMMAND ----------

import sys
import os
sys.dont_write_bytecode = True
os.environ['PYTHONDONTWRITEBYTECODE'] = '1'
sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")

from tests.run_tests import run_all_tests
run_all_tests() 

# COMMAND ----------

# Run Specific Layers
from tests.run_tests import run_silver_tests, run_gold_tests

run_silver_tests()  # Only Silver layer
run_gold_tests()    # Only Gold layer

# COMMAND ----------

#Run Specific Tests
from tests.run_tests import run_specific_test

# Run one test class
run_specific_test("test_silver.py::TestCleanThreadLookup")

# Run one test method
run_specific_test("test_gold.py::TestGoldUserTeamMetrics::test_calculates_checkpoint_count")

# COMMAND ----------

# Run unit tests
from tests.run_tests import run_unit_tests
run_unit_tests()