"""
Centralized configuration for the chatbot pipeline.

This module provides configuration classes and constants used across
Bronze, Silver, and Gold layers of the pipeline.
"""

from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType


class CatalogConfig:
    """Unity Catalog configuration."""
    CATALOG = "main"
    SCHEMA = "chat_history"
    
    @classmethod
    def get_full_table_name(cls, table_name: str) -> str:
        """
        Get fully qualified table name.
        
        Args:
            table_name: The table name (without catalog/schema prefix)
            
        Returns:
            Fully qualified name in format: catalog.schema.table
        """
        return f"{cls.CATALOG}.{cls.SCHEMA}.{table_name}"


class TableNames:
    """Table names for each pipeline layer."""
    
    # Bronze layer
    BRONZE_CHECKPOINTS = "bronze_checkpoints"
    BRONZE_THREAD_LOOKUP_STREAM = "bronze_thread_lookup_stream"
    
    # Silver layer
    SILVER_CHECKPOINTS = "silver_checkpoints"
    SILVER_THREAD_LOOKUP_HISTORY = "silver_thread_lookup_history"
    
    # Gold layer
    GOLD_USER_TEAM_METRICS = "gold_user_team_metrics"
    GOLD_TEAM_SUMMARY = "gold_team_summary"
    
    # Monitoring tables
    MONITORING_DAILY_COUNTS = "monitoring_daily_counts"
    MONITORING_DUPLICATES = "monitoring_duplicates"
    MONITORING_NULL_PERCENTAGES = "monitoring_null_percentages"
    MONITORING_PROCESSING_LAG = "monitoring_processing_lag"
    
    @classmethod
    def get_all_tables(cls) -> list:
        """Get list of all table names."""
        return [
            cls.BRONZE_CHECKPOINTS,
            cls.BRONZE_THREAD_LOOKUP_STREAM,
            cls.SILVER_CHECKPOINTS,
            cls.SILVER_THREAD_LOOKUP_HISTORY,
            cls.GOLD_USER_TEAM_METRICS,
            cls.GOLD_TEAM_SUMMARY,
            cls.MONITORING_DAILY_COUNTS,
            cls.MONITORING_DUPLICATES,
            cls.MONITORING_NULL_PERCENTAGES,
            cls.MONITORING_PROCESSING_LAG
        ]
    
    @classmethod
    def get_all_qualified_tables(cls) -> list:
        """Get list of all fully qualified table names."""
        return [CatalogConfig.get_full_table_name(t) for t in cls.get_all_tables()]
    
    @classmethod
    def get_gold_tables(cls) -> list:
        """Get list of Gold layer table names."""
        return [
            cls.GOLD_USER_TEAM_METRICS,
            cls.GOLD_TEAM_SUMMARY
        ]
    
    @classmethod
    def get_streaming_tables(cls) -> list:
        """Get list of streaming table names."""
        return [
            cls.BRONZE_CHECKPOINTS,
            cls.BRONZE_THREAD_LOOKUP_STREAM,
            cls.SILVER_THREAD_LOOKUP_HISTORY
        ]
    
    @classmethod
    def get_monitoring_tables(cls) -> list:
        """Get list of monitoring table names."""
        return [
            cls.MONITORING_DAILY_COUNTS,
            cls.MONITORING_DUPLICATES,
            cls.MONITORING_NULL_PERCENTAGES,
            cls.MONITORING_PROCESSING_LAG
        ]


class SchemaConfig:
    """Schema definitions for JSON parsing and data validation."""
    
    # Checkpoint JSON schema
    CHECKPOINT_SCHEMA = StructType([
        StructField("ts", StringType(), True),
        StructField("v", StringType(), True),
        StructField("id", StringType(), True),
        StructField("channel_versions", MapType(StringType(), StringType()), True)
    ])
    
    # Metadata JSON schema
    METADATA_SCHEMA = StructType([
        StructField("step", StringType(), True),
        StructField("source", StringType(), True)
    ])


class MaintenanceConfig:
    """Configuration for table maintenance operations."""
    
    # VACUUM retention period (in hours)
    VACUUM_RETENTION_HOURS = 720  # 30 days (recommended for production)
    
    # Optimization settings
    AUTO_OPTIMIZE_ENABLED = True
    AUTO_COMPACT_ENABLED = True
    
    # Data skipping settings (Delta optimization)
    DATA_SKIPPING_ENABLED = True
    DATA_SKIPPING_NUM_INDEXED_COLS = 32
    
    # Deletion vector settings
    DELETION_VECTORS_ENABLED = True
    
    # Z-order columns by table (for non-liquid-clustered tables)
    ZORDER_COLUMNS = {
        TableNames.BRONZE_CHECKPOINTS: ["thread_id", "checkpoint_ns"],
        TableNames.BRONZE_THREAD_LOOKUP_STREAM: ["thread_id"],
        TableNames.SILVER_CHECKPOINTS: ["thread_id", "checkpoint_date"],
        TableNames.SILVER_THREAD_LOOKUP_HISTORY: ["thread_id", "__start_at"],
        TableNames.MONITORING_DAILY_COUNTS: ["pipeline_layer", "checkpoint_date"],
        TableNames.MONITORING_DUPLICATES: ["source_layer"],
        TableNames.MONITORING_NULL_PERCENTAGES: ["layer", "table_name"],
        TableNames.MONITORING_PROCESSING_LAG: ["checkpoint_date"]
    }
    
    # Partition columns by table
    PARTITION_COLUMNS = {
        TableNames.GOLD_USER_TEAM_METRICS: ["checkpoint_date"],
        TableNames.GOLD_TEAM_SUMMARY: ["checkpoint_date"]
    }
    
    # Liquid clustering columns (for Gold tables)
    LIQUID_CLUSTERING_COLUMNS = {
        TableNames.GOLD_USER_TEAM_METRICS: ["checkpoint_date", "team", "user_id"],
        TableNames.GOLD_TEAM_SUMMARY: ["checkpoint_date", "team"]
    }
    
    @classmethod
    def get_table_properties(cls, table_name: str = None) -> dict:
        """
        Get recommended table properties for Delta tables.
        
        Args:
            table_name: Optional table name for table-specific properties
            
        Returns:
            Dictionary of table properties
        """
        base_props = {
            'delta.autoOptimize.optimizeWrite': str(cls.AUTO_OPTIMIZE_ENABLED).lower(),
            'delta.autoOptimize.autoCompact': str(cls.AUTO_COMPACT_ENABLED).lower(),
            'delta.enableDeletionVectors': str(cls.DELETION_VECTORS_ENABLED).lower(),
            'delta.dataSkippingNumIndexedCols': str(cls.DATA_SKIPPING_NUM_INDEXED_COLS),
        }
        
        # Add table-specific properties for Gold tables
        if table_name in TableNames.get_gold_tables():
            base_props.update({
                'delta.targetFileSize': '128mb',  # Larger files for Gold aggregations
                'delta.tuneFileSizesForRewrites': 'true',  # Optimize file sizes during rewrites
                'delta.checkpoint.writeStatsAsJson': 'false',  # More compact checkpoints
                'delta.checkpoint.writeStatsAsStruct': 'true'
            })
        
        # Streaming tables: smaller target file size, more frequent checkpoints
        if table_name in TableNames.get_streaming_tables():
            base_props.update({
                'delta.targetFileSize': '64mb',  # Smaller files for streaming append
                'delta.logRetentionDuration': 'interval 30 days',
                'delta.deletedFileRetentionDuration': 'interval 30 days'
            })
        
        return base_props
    
    @classmethod
    def get_partition_columns(cls, table_name: str) -> list:
        """
        Get partition columns for a specific table.
        
        Args:
            table_name: Table name (without catalog/schema prefix)
            
        Returns:
            List of partition column names, or empty list if not partitioned
        """
        return cls.PARTITION_COLUMNS.get(table_name, [])
    
    @classmethod
    def should_use_liquid_clustering(cls, table_name: str) -> bool:
        """
        Check if table should use liquid clustering.
        
        Args:
            table_name: Table name (without catalog/schema prefix)
            
        Returns:
            True if liquid clustering is recommended
        """
        # Liquid clustering is recommended for Gold tables
        return table_name in TableNames.get_gold_tables()


class PipelineConfig:
    """General pipeline configuration settings."""
    
    # Pipeline mode
    SERVERLESS = True
    PHOTON_ENABLED = True
    
    # Default checkpoint location
    CHECKPOINT_LOCATION = "/tmp/chatbot_pipeline_checkpoints"
    
    # Streaming trigger interval (if not continuous)
    TRIGGER_INTERVAL = "5 minutes"
    
    # Data quality actions
    EXPECTATIONS_ACTION = "quarantine"  # Options: "warn", "drop", "quarantine", "fail"


# Convenience exports for common usage
CATALOG = CatalogConfig.CATALOG
SCHEMA = CatalogConfig.SCHEMA
TABLES = TableNames
