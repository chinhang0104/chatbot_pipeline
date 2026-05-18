from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

checkpoint_schema = StructType([
    StructField("id", StringType(), True),
    StructField("status", StringType(), True),
])

metadata_schema = StructType([
    StructField("source", StringType(), True),
    StructField("version", StringType(), True),
])

@dp.table(
    comment="Bronze: Auto Loader checkpoints (incremental), raw + parsed JSON",
    table_properties={
        "quality": "bronze",
        "delta.autoOptimize.optimizeWrite": "true"
    }
)
def bronze_checkpoints():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("cloudFiles.schemaHints", "thread_id string, checkpoint_ns string, checkpoint_id string, parent_checkpoint_id string, type string, checkpoint string, metadata string")
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle new columns
        .option("rescuedDataColumn", "_rescued_data")  # Keep bad rows
        .load("/Volumes/main/chat_history/raw_data/checkpoints_*.csv")
        .withColumn("ingested_at", F.current_timestamp())
    )
    
@dp.table(
    comment="Bronze: Thread lookup (Auto Loader incremental from CSV)",
    table_properties={"quality": "bronze"}
)
def bronze_thread_lookup_stream():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.schemaHints", "thread_id string, user_id string, team string")
        .load("/Volumes/main/chat_history/raw_data/thread_teams_*.csv")
        .withColumn("ingested_at", F.current_timestamp())
    )
