"""
Table maintenance utilities for the chatbot pipeline.

This script performs OPTIMIZE, table property configuration, health checks,
and analysis operations on all pipeline tables to maintain performance.

For VACUUM operations (storage cleanup), see vacuum_tables.py

Usage:
    # In a Databricks notebook
    %run /Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline/utilities/maintenance
    
    # Run all maintenance tasks
    run_all_maintenance()
    
    # Run specific tasks
    optimize_all_tables()
    configure_all_tables()
    analyze_all_tables()
    show_table_health()
    
    # Preview commands without executing
    run_all_maintenance(dry_run=True)
    
    # Display optimization recommendations
    show_optimization_recommendations()
"""

import sys
sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")

from config import CatalogConfig, TableNames, MaintenanceConfig
from datetime import datetime
from pyspark.sql import functions as F


def optimize_table(table_name: str, use_liquid_clustering: bool = False, zorder_columns: list = None, dry_run: bool = False):
    """
    Optimize a specific table.
    
    For liquid clustered tables, runs OPTIMIZE without Z-ordering (auto-managed).
    For non-clustered tables, optionally applies Z-ordering.
    
    Args:
        table_name: Fully qualified table name
        use_liquid_clustering: If True, skip Z-ordering (table uses liquid clustering)
        zorder_columns: Optional list of columns to Z-order by (ignored if use_liquid_clustering=True)
        dry_run: If True, only print commands without executing
    """
    try:
        if use_liquid_clustering:
            # Liquid clustering auto-manages clustering during OPTIMIZE
            sql = f"OPTIMIZE {table_name}"
            optimization_note = " (liquid clustering auto-managed)"
        elif zorder_columns:
            zorder_clause = ", ".join(zorder_columns)
            sql = f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})"
            optimization_note = f" with Z-ORDER BY ({zorder_clause})"
        else:
            sql = f"OPTIMIZE {table_name}"
            optimization_note = ""
        
        if dry_run:
            print(f"[DRY RUN] Would execute: {sql}")
        else:
            spark.sql(sql)
            print(f"✓ Optimized {table_name}{optimization_note}")
    except Exception as e:
        print(f"✗ Failed to optimize {table_name}: {e}")


def set_table_properties(table_name: str, base_table_name: str = None, dry_run: bool = False):
    """
    Set recommended table properties for optimization.
    
    Args:
        table_name: Fully qualified table name
        base_table_name: Base table name (without catalog/schema) for table-specific properties
        dry_run: If True, only print commands without executing
    """
    try:
        props = MaintenanceConfig.get_table_properties(base_table_name)
        for key, value in props.items():
            sql = f"ALTER TABLE {table_name} SET TBLPROPERTIES ('{key}' = '{value}')"
            if dry_run:
                print(f"[DRY RUN] Would execute: {sql}")
            else:
                spark.sql(sql)
        
        if not dry_run:
            print(f"✓ Set table properties for {table_name}")
    except Exception as e:
        print(f"✗ Failed to set properties for {table_name}: {e}")


def analyze_table(table_name: str, dry_run: bool = False):
    """
    Compute statistics for a table to improve query performance.
    
    Args:
        table_name: Fully qualified table name
        dry_run: If True, only print commands without executing
    """
    try:
        sql = f"ANALYZE TABLE {table_name} COMPUTE STATISTICS FOR ALL COLUMNS"
        if dry_run:
            print(f"[DRY RUN] Would execute: {sql}")
        else:
            spark.sql(sql)
            print(f"✓ Analyzed {table_name}")
    except Exception as e:
        print(f"✗ Failed to analyze {table_name}: {e}")


def get_table_health(table_name: str) -> dict:
    """
    Get health metrics for a table.
    
    Args:
        table_name: Fully qualified table name
        
    Returns:
        Dictionary with health metrics (size, file count, etc.)
    """
    try:
        # Get table details
        detail_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        detail = detail_df.first()
        
        # Get history count
        history_count = spark.sql(f"DESCRIBE HISTORY {table_name}").count()
        
        return {
            "name": table_name,
            "size_mb": round(detail.sizeInBytes / (1024 * 1024), 2) if detail.sizeInBytes else 0,
            "num_files": detail.numFiles if detail.numFiles else 0,
            "format": detail.format,
            "created_at": detail.createdAt.strftime("%Y-%m-%d %H:%M") if detail.createdAt else "N/A",
            "last_modified": detail.lastModified.strftime("%Y-%m-%d %H:%M") if detail.lastModified else "N/A",
            "version_count": history_count
        }
    except Exception as e:
        return {
            "name": table_name,
            "error": str(e)
        }


def show_table_health():
    """
    Display health metrics for all pipeline tables.
    
    Shows table sizes, file counts, versions, and potential issues.
    """
    print(f"\n{'=' * 100}")
    print(" " * 38 + "TABLE HEALTH REPORT")
    print(f"{'=' * 100}\n")
    
    health_data = []
    total_size_mb = 0
    total_files = 0
    
    for table in TableNames.get_all_qualified_tables():
        health = get_table_health(table)
        health_data.append(health)
        
        if "error" not in health:
            total_size_mb += health["size_mb"]
            total_files += health["num_files"]
    
    # Print header
    print(f"{'Table':<50} {'Size (MB)':<12} {'Files':<8} {'Versions':<10} {'Last Modified'}")
    print("-" * 100)
    
    # Print each table
    for health in health_data:
        if "error" in health:
            print(f"{health['name']:<50} {'ERROR':<12} {'-':<8} {'-':<10} {health['error'][:20]}")
        else:
            # Highlight potential issues
            file_warning = "⚠️ " if health["num_files"] > 1000 else ""
            size_display = f"{file_warning}{health['size_mb']:,.2f}"
            
            print(f"{health['name']:<50} {size_display:<12} {health['num_files']:<8} "
                  f"{health['version_count']:<10} {health['last_modified']}")
    
    # Print summary
    print("-" * 100)
    print(f"{'TOTAL':<50} {total_size_mb:,.2f} MB    {total_files} files\n")
    
    # Print warnings
    print("💡 Health Indicators:")
    print("   • ⚠️ Tables with >1000 files should be optimized")
    print("   • Large version counts indicate frequent updates")
    print("   • Zero files may indicate empty tables or metadata-only views")
    print("   • Run vacuum_tables.py to clean up old file versions")
    print(f"\n{'=' * 100}\n")


def cleanup_streaming_checkpoints(dry_run: bool = False):
    """
    Display streaming checkpoint information.
    
    Streaming tables maintain checkpoint metadata that can grow over time.
    This function helps identify checkpoint locations for manual cleanup if needed.
    
    Args:
        dry_run: If True, only display checkpoint info (always non-destructive)
    """
    print(f"\n{'=' * 70}")
    print(" " * 15 + "STREAMING CHECKPOINT STATUS")
    print(f"{'=' * 70}\n")
    
    print("📊 Streaming Tables:")
    print("-" * 70)
    
    for table in TableNames.get_streaming_tables():
        full_name = CatalogConfig.get_full_table_name(table)
        
        try:
            detail_df = spark.sql(f"DESCRIBE DETAIL {full_name}")
            detail = detail_df.first()
            
            print(f"\n✓ {full_name}")
            print(f"   Location: {detail.location}")
            print(f"   Format: {detail.format}")
            print(f"   💡 Checkpoint location typically at: {detail.location}/_checkpoints/")
            print(f"      Manual cleanup may be needed for very old checkpoints")
            
        except Exception as e:
            print(f"\n✗ {full_name}: {e}")
    
    print(f"\n{'-' * 70}")
    print("\n💡 Note:")
    print("   • Streaming checkpoints are managed automatically by Spark")
    print("   • Manual cleanup only needed in exceptional circumstances")
    print("   • Contact your admin before manually removing checkpoint files")
    print(f"\n{'=' * 70}\n")


def show_optimization_recommendations():
    """
    Display current optimization strategy for all tables.
    
    Shows which tables use liquid clustering vs Z-ordering, and provides
    recommendations for tables not yet using liquid clustering.
    """
    print(f"\n{'=' * 80}")
    print(" " * 22 + "TABLE OPTIMIZATION STRATEGY")
    print(f"{'=' * 80}\n")
    
    print("📊 Gold Tables - Liquid Clustering (ACTIVE)")
    print("-" * 80)
    
    for table in TableNames.get_gold_tables():
        full_name = CatalogConfig.get_full_table_name(table)
        partition_cols = MaintenanceConfig.get_partition_columns(table)
        clustering_cols = MaintenanceConfig.LIQUID_CLUSTERING_COLUMNS.get(table, [])
        
        print(f"\n✅ {full_name}")
        print(f"   Strategy: Liquid Clustering (auto-optimizes during OPTIMIZE)")
        
        if partition_cols:
            print(f"   Partitioned By: {', '.join(partition_cols)}")
        
        if clustering_cols:
            print(f"   Clustered By: {', '.join(clustering_cols)}")
            print(f"   ├─ Auto-optimizes data layout during writes")
            print(f"   ├─ No Z-ordering needed")
            print(f"   └─ Ideal for evolving query patterns")
    
    print(f"\n\n📊 Bronze/Silver/Monitoring Tables - Z-Ordering (ACTIVE)")
    print("-" * 80)
    
    for table in TableNames.get_all_tables():
        if table not in TableNames.get_gold_tables():
            full_name = CatalogConfig.get_full_table_name(table)
            zorder_cols = MaintenanceConfig.ZORDER_COLUMNS.get(table, [])
            is_streaming = table in TableNames.get_streaming_tables()
            
            print(f"\n  {full_name}")
            if is_streaming:
                print(f"   Type: Streaming Table")
            if zorder_cols:
                print(f"   Current Z-order: {', '.join(zorder_cols)}")
                print(f"   💡 Consider migrating to liquid clustering for better performance")
            else:
                print(f"   No Z-ordering configured")
    
    print(f"\n{'-' * 80}")
    print("\n💡 Key Points:")
    print("   • Gold tables: Already using liquid clustering ✅")
    print("   • Liquid clustering: Auto-manages data layout, no manual Z-ordering needed")
    print("   • OPTIMIZE: Triggers clustering optimization automatically")
    print("   • Bronze/Silver: Can be migrated to liquid clustering by recreating tables")
    print(f"\n{'=' * 80}\n")


def optimize_all_tables(dry_run: bool = False):
    """
    Optimize all pipeline tables.
    
    Gold tables use liquid clustering (no Z-ordering needed).
    Other tables use Z-ordering where configured.
    
    Args:
        dry_run: If True, only print commands without executing
    """
    print(f"\n{'=' * 70}")
    print(" " * 20 + "OPTIMIZING TABLES")
    if dry_run:
        print(" " * 25 + "[DRY RUN MODE]")
    print(f"{'=' * 70}\n")
    
    for table in TableNames.get_all_tables():
        full_name = CatalogConfig.get_full_table_name(table)
        
        # Gold tables use liquid clustering (skip Z-ordering)
        if MaintenanceConfig.should_use_liquid_clustering(table):
            optimize_table(full_name, use_liquid_clustering=True, dry_run=dry_run)
        else:
            # Bronze/Silver/Monitoring tables use Z-ordering
            zorder_cols = MaintenanceConfig.ZORDER_COLUMNS.get(table)
            optimize_table(full_name, use_liquid_clustering=False, zorder_columns=zorder_cols, dry_run=dry_run)
    
    print(f"\n{'=' * 70}\n")


def configure_all_tables(dry_run: bool = False):
    """
    Set recommended properties for all tables.
    
    Args:
        dry_run: If True, only print commands without executing
    """
    print(f"\n{'=' * 70}")
    print(" " * 15 + "CONFIGURING TABLE PROPERTIES")
    if dry_run:
        print(" " * 25 + "[DRY RUN MODE]")
    print(f"{'=' * 70}\n")
    
    for table in TableNames.get_all_tables():
        full_name = CatalogConfig.get_full_table_name(table)
        set_table_properties(full_name, base_table_name=table, dry_run=dry_run)
    
    print(f"\n{'=' * 70}\n")


def analyze_all_tables(dry_run: bool = False):
    """
    Analyze all tables to compute statistics.
    
    Args:
        dry_run: If True, only print commands without executing
    """
    print(f"\n{'=' * 70}")
    print(" " * 20 + "ANALYZING TABLES")
    if dry_run:
        print(" " * 25 + "[DRY RUN MODE]")
    print(f"{'=' * 70}\n")
    
    for table in TableNames.get_all_qualified_tables():
        analyze_table(table, dry_run=dry_run)
    
    print(f"\n{'=' * 70}\n")


def run_all_maintenance(include_analysis: bool = False, dry_run: bool = False):
    """
    Run complete maintenance workflow.
    
    This runs:
    - OPTIMIZE (liquid clustering for Gold, Z-ordering for Bronze/Silver)
    - Table property configuration
    - Optional: Table analysis (statistics)
    
    For VACUUM operations (storage cleanup), see vacuum_tables.py
    
    Args:
        include_analysis: Whether to include table analysis (can be slow)
        dry_run: If True, only print commands without executing
    """
    start_time = datetime.now()
    
    print(f"\n{'=' * 70}")
    print(" " * 15 + "CHATBOT PIPELINE MAINTENANCE")
    if dry_run:
        print(" " * 25 + "[DRY RUN MODE]")
    print(f"{'=' * 70}")
    print(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    optimize_all_tables(dry_run=dry_run)
    configure_all_tables(dry_run=dry_run)
    
    if include_analysis:
        analyze_all_tables(dry_run=dry_run)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"{'=' * 70}")
    print(f"✅ Maintenance completed in {duration:.1f} seconds")
    if dry_run:
        print("   [DRY RUN - No changes were made]")
    print(f"\n💡 Additional Commands:")
    print(f"   • show_table_health() - View table sizes and file counts")
    print(f"   • show_optimization_recommendations() - View optimization strategy")
    print(f"   • cleanup_streaming_checkpoints() - View streaming checkpoint info")
    print(f"   • %run ./vacuum_tables - Run VACUUM operations (separate file)")
    print(f"{'=' * 70}\n")


# Auto-run when executed directly
if __name__ == "__main__":
    run_all_maintenance()
