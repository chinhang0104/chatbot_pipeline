import dlt
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

@dlt.table(
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
        .option("cloudFiles.schemaLocation", "/Volumes/main/chat_history/_checkpoints_schema")  # Schema evolution
        .option("cloudFiles.schemaEvolutionMode", "rescue")  # Handle new columns
        .option("rescuedDataColumn", "_rescued_data")  # Keep bad rows
        .load("/Volumes/main/chat_history/raw_data/checkpoints_*.csv")
        .withColumn("ingested_at", F.current_timestamp())
    )

    
# Dont use auto loader for mutable table
# @dlt.table(
#     comment="Bronze: Auto Loader thread lookup (incremental)",
#     table_properties={
#         "quality": "bronze",
#         "delta.autoOptimize.optimizeWrite": "true"
#     }
# )
# def bronze_thread_lookup():
#     return (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("header", "true")
#         .option("cloudFiles.schemaLocation", "/Volumes/main/chat_history/_lookup_schema")
#         .load("/Volumes/main/chat_history/raw_data/thread_lookup_*.csv")
#         .withColumn("ingested_at", F.current_timestamp())
#     )