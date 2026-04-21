import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StructType, StructField, StringType, MapType

# @dlt.table(
#     comment="Silver: cleaned thread lookup",
#     table_properties={"quality": "silver"}
# )
# @dlt.expect_or_drop("thread_id_not_null", "thread_id IS NOT NULL")
# def silver_thread_lookup():
#     src = dlt.read("bronze_thread_lookup")
#     return (
#         src.select(
#             F.col("thread_id").cast("string").alias("thread_id"),
#             F.col("user_id").cast("string").alias("user_id"),
#             F.col("team").cast("string").alias("team")
#         )
#         .dropDuplicates(["thread_id"])
#     )
LOOKUP_PATH = "/Volumes/main/chat_history/raw_data/thread_lookup.csv"
LOOKUP_MTIME_PROP = "lookup_file_mtime"

def get_lookup_file_mtime():
    """Get thread_lookup.csv modification time (epoch seconds)"""
    try:
        files = dbutils.fs.ls("/Volumes/main/chat_history/raw_data/")
        lookup_file = next(f for f in files if f.name == "thread_lookup.csv")
        return lookup_file.modificationTime / 1000.0  # Convert ms to seconds
    except:
        return 0.0

def should_skip_lookup():
    """Skip processing if lookup file unchanged (using Delta table properties)"""
    try:
        # Get last processed mtime from table properties
        props_df = spark.sql("SHOW TBLPROPERTIES main.chat_history.silver_thread_lookup")
        props = {row['key']: row['value'] for row in props_df.collect()}
        last_mtime = float(props.get(LOOKUP_MTIME_PROP, '0'))
    except:
        last_mtime = 0.0
    
    current_mtime = get_lookup_file_mtime()
    changed = current_mtime > last_mtime + 1.0  # 1s tolerance
    
    if changed:
        # Update table property with new mtime
        spark.sql(f"""
            ALTER TABLE main.chat_history.silver_thread_lookup 
            SET TBLPROPERTIES (
                '{LOOKUP_MTIME_PROP}' = '{current_mtime}'
            )
        """)
    
    return not changed

@dlt.table(
    comment="Silver: SCD Type 1 thread lookup (MERGE overwrites)",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("thread_id_not_null", "thread_id IS NOT NULL")
def silver_thread_lookup():
    if should_skip_lookup():
        # Return empty DataFrame → DLT skips downstream processing
        return spark.createDataFrame([], 
            schema="thread_id string, user_id string, team string, updated_at timestamp")
    
    # Process changed lookup file
    new_lookup = (
        spark.read.format("csv")
        .option("header", "true")
        .load("/Volumes/main/chat_history/raw_data/thread_lookup.csv")
        .select(
            F.col("thread_id").cast("string").alias("thread_id"),
            F.col("user_id").cast("string").alias("user_id"),
            F.col("team").cast("string").alias("team"),
            F.current_timestamp().alias("updated_at")
        )
    )
    
    # SCD Type 1: Overwrite existing teams
    return new_lookup

@dlt.table(
    comment="Silver: checkpoints enriched with user/team",
    table_properties={"quality": "silver"}
)
@dlt.expect_or_drop("checkpoint_ts_not_null", "checkpoint_ts IS NOT NULL")
def silver_checkpoints():
    b = dlt.read("bronze_checkpoints")
    dim = dlt.read("silver_thread_lookup")

    checkpoint_schema = StructType([
        StructField("ts", StringType(), True),
        StructField("v", StringType(), True),
        StructField("id", StringType(), True),
        StructField("channel_versions", MapType(StringType(), StringType()), True)
    ])
    
    metadata_schema = StructType([
        StructField("step", StringType(), True),
        StructField("source", StringType(), True)
    ])

    df = (
        b
        .withColumn("checkpoint_parsed", F.from_json(F.col("checkpoint"), checkpoint_schema))
        .withColumn("metadata_parsed", F.from_json(F.col("metadata"), metadata_schema))
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

    return df