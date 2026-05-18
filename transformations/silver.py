from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, MapType
from transformations.transforms import clean_thread_lookup, enrich_checkpoints

# Preprocessing temporary view for thread lookup
@dp.temporary_view(
    comment="Temporary view: cleaned thread lookup for CDC processing"
)
@dp.expect_or_drop("thread_id_not_null", "thread_id IS NOT NULL")
def thread_lookup_cleaned():
    return clean_thread_lookup(spark.readStream.table("bronze_thread_lookup_stream"))

# Target streaming table for Auto CDC with SCD Type 2 (renamed to avoid conflict with existing Type 1 table)
dp.create_streaming_table(
    name="silver_thread_lookup_history",
    comment="Silver: SCD Type 2 thread lookup with full change history (Auto CDC)",
    table_properties={"quality": "silver"}
)

# Auto CDC flow: apply changes from bronze to silver with full history
dp.create_auto_cdc_flow(
    target="silver_thread_lookup_history",
    source="thread_lookup_cleaned",
    keys=["thread_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)

@dp.materialized_view(
    comment="Silver: checkpoints enriched with user/team",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop("checkpoint_ts_not_null", "checkpoint_ts IS NOT NULL")
def silver_checkpoints():
    # Read only current records from SCD Type 2 table
    current_thread_lookup = spark.read.table("silver_thread_lookup_history").filter(F.col("__END_AT").isNull())
    return enrich_checkpoints(spark.read.table("bronze_checkpoints"), current_thread_lookup)
