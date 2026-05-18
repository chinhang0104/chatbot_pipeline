"""
VACUUM operations for chatbot pipeline tables.

⚠️ WARNING: VACUUM is a DESTRUCTIVE operation!
   - Permanently deletes old file versions
   - Makes time travel queries impossible beyond the retention period
   - Cannot be undone
   - Should only be run after careful consideration

Recommended retention periods:
   - Production tables: 30 days (720 hours) - default
   - Development/Testing: 7 days (168 hours)
   - Tables with frequent rollbacks: 90 days (2160 hours)

Usage:
    # In a Databricks notebook
    %run /Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline/utilities/vacuum_tables
    
    # Step 1: Analyze impact across all tables
    show_vacuum_impact_analysis()
    
    # Step 2: Preview specific table
    show_vacuum_preview("main.chat_history.bronze_checkpoints")
    
    # Step 3: Dry run on all tables
    vacuum_all_pipeline_tables(dry_run=True)
    
    # Step 4: Execute with explicit confirmation
    vacuum_all_pipeline_tables(retention_hours=720, dry_run=False)
    
    # Single table VACUUM
    vacuum_table("main.chat_history.bronze_checkpoints", retention_hours=720, dry_run=False)
"""

import sys
sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")

from config import CatalogConfig, TableNames, MaintenanceConfig
from datetime import datetime, timedelta
from typing import List


def show_vacuum_preview(table_name: str, retention_hours: int = None):
    """
    Preview files that would be deleted by VACUUM on a table.
    
    Shows which files would be removed and estimates storage savings.
    
    Args:
        table_name: Fully qualified table name
        retention_hours: Retention period (default: from MaintenanceConfig)
    """
    if retention_hours is None:
        retention_hours = MaintenanceConfig.VACUUM_RETENTION_HOURS
    
    print(f"\n{'=' * 80}")
    print(f"VACUUM PREVIEW: {table_name}")
    print(f"{'=' * 80}\n")
    print(f"Retention: {retention_hours} hours ({retention_hours // 24} days)\n")
    
    try:
        # Run VACUUM DRY RUN to see what would be deleted
        preview_df = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
        file_count = preview_df.count()
        
        print(f"📁 Files to be deleted: {file_count}")
        
        if file_count > 0:
            print("\n⚠️  These files will be PERMANENTLY deleted:")
            display(preview_df)
        else:
            print("✓ No files to delete (all files are within retention period)")
        
        # Show table history
        print(f"\n📊 Table History:")
        history_df = spark.sql(f"DESCRIBE HISTORY {table_name}")
        display(history_df.select("version", "timestamp", "operation", "operationMetrics"))
        
    except Exception as e:
        print(f"✗ Error previewing VACUUM: {e}")
    
    print(f"\n{'=' * 80}\n")


def vacuum_table(table_name: str, retention_hours: int = None, dry_run: bool = True):
    """
    Run VACUUM on a single table with retention and dry_run options.
    
    ⚠️ WARNING: When dry_run=False, this PERMANENTLY deletes old file versions!
    
    Args:
        table_name: Fully qualified table name
        retention_hours: Retention period (default: from MaintenanceConfig)
        dry_run: If True, only shows what would be deleted (DEFAULT: True)
    """
    if retention_hours is None:
        retention_hours = MaintenanceConfig.VACUUM_RETENTION_HOURS
    
    print(f"\n{'=' * 80}")
    print(f"{'[DRY RUN] ' if dry_run else '⚠️  [EXECUTING] '}VACUUM: {table_name}")
    print(f"{'=' * 80}\n")
    
    try:
        # Get before state
        detail_before = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
        history_before = spark.sql(f"DESCRIBE HISTORY {table_name}")
        version_count_before = history_before.count()
        oldest_version = history_before.orderBy("timestamp").first()
        
        print(f"📊 Table Status BEFORE:")
        print(f"   Size: {detail_before.sizeInBytes / (1024**3):.2f} GB")
        print(f"   Files: {detail_before.numFiles}")
        print(f"   Versions: {version_count_before}")
        print(f"   Oldest version: {oldest_version.timestamp.strftime('%Y-%m-%d %H:%M')}")
        
        # Execute VACUUM
        if dry_run:
            print(f"\n🔍 DRY RUN - Previewing files to delete (retention: {retention_hours} hours)...")
            preview_df = spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS DRY RUN")
            file_count = preview_df.count()
            print(f"   Would delete {file_count} files")
            
            if file_count > 0 and file_count <= 20:
                display(preview_df)
            elif file_count > 20:
                print(f"   (Too many files to display - showing first 20)")
                display(preview_df.limit(20))
        else:
            print(f"\n⚠️  EXECUTING VACUUM (retention: {retention_hours} hours)...")
            print("   This will PERMANENTLY delete old file versions!")
            
            start_time = datetime.now()
            spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
            duration = (datetime.now() - start_time).total_seconds()
            
            print(f"   ✓ VACUUM completed in {duration:.1f} seconds")
            
            # Get after state
            detail_after = spark.sql(f"DESCRIBE DETAIL {table_name}").first()
            history_after = spark.sql(f"DESCRIBE HISTORY {table_name}")
            version_count_after = history_after.count()
            
            size_freed = detail_before.sizeInBytes - detail_after.sizeInBytes
            files_removed = detail_before.numFiles - detail_after.numFiles
            
            print(f"\n📊 Table Status AFTER:")
            print(f"   Size: {detail_after.sizeInBytes / (1024**3):.2f} GB (freed: {size_freed / (1024**3):.2f} GB)")
            print(f"   Files: {detail_after.numFiles} (removed: {files_removed})")
            print(f"   Versions: {version_count_after}")
            
            print(f"\n⚠️  Time travel now limited to past {retention_hours // 24} days")
        
    except Exception as e:
        print(f"✗ Error during VACUUM: {e}")
    
    print(f"\n{'=' * 80}\n")


def show_vacuum_impact_analysis(retention_hours: int = None):
    """
    Analyze VACUUM impact across all pipeline tables.
    
    Shows which tables would be affected and estimated storage savings.
    
    Args:
        retention_hours: Retention period to analyze (default: from MaintenanceConfig)
    """
    if retention_hours is None:
        retention_hours = MaintenanceConfig.VACUUM_RETENTION_HOURS
    
    cutoff_date = datetime.now() - timedelta(hours=retention_hours)
    
    print(f"\n{'=' * 100}")
    print(" " * 30 + "VACUUM IMPACT ANALYSIS")
    print(f"{'=' * 100}\n")
    print(f"Retention Period: {retention_hours} hours ({retention_hours // 24} days)")
    print(f"Cutoff Date: {cutoff_date.strftime('%Y-%m-%d %H:%M')}")
    print(f"Files older than this will be PERMANENTLY deleted\n")
    print(f"{'=' * 100}\n")
    
    # Header
    print(f"{'Table':<50} {'Versions':<10} {'Oldest Version':<20} {'Impact'}")
    print("-" * 100)
    
    total_tables = 0
    tables_with_old_data = 0
    
    for table in TableNames.get_all_qualified_tables():
        try:
            history_df = spark.sql(f"DESCRIBE HISTORY {table}")
            version_count = history_df.count()
            oldest = history_df.orderBy("timestamp").first()
            oldest_date = oldest.timestamp if oldest else None
            
            total_tables += 1
            
            if oldest_date and oldest_date < cutoff_date:
                impact = f"⚠️  {(datetime.now() - oldest_date).days} days old"
                tables_with_old_data += 1
            else:
                impact = "✓ Within retention"
            
            oldest_str = oldest_date.strftime("%Y-%m-%d %H:%M") if oldest_date else "N/A"
            print(f"{table:<50} {version_count:<10} {oldest_str:<20} {impact}")
            
        except Exception as e:
            print(f"{table:<50} {'ERROR':<10} {str(e)[:38]}")
    
    print("-" * 100)
    print(f"\n📊 Summary:")
    print(f"   Total tables: {total_tables}")
    print(f"   Tables with data beyond retention: {tables_with_old_data}")
    print(f"   Tables within retention: {total_tables - tables_with_old_data}")
    
    print(f"\n⚠️  Impact:")
    if tables_with_old_data > 0:
        print(f"   • {tables_with_old_data} tables have versions older than {retention_hours // 24} days")
        print(f"   • VACUUM will permanently delete those old versions")
        print(f"   • Time travel queries beyond {retention_hours // 24} days will FAIL")
    else:
        print(f"   • All tables are within the {retention_hours // 24}-day retention period")
        print(f"   • VACUUM will have minimal impact (no old files to delete)")
    
    print(f"\n💡 Next Steps:")
    print(f"   1. Review tables marked with ⚠️  above")
    print(f"   2. Use show_vacuum_preview(table_name) to see specific files")
    print(f"   3. Run vacuum_all_pipeline_tables(dry_run=True) to preview")
    print(f"   4. If satisfied, run vacuum_all_pipeline_tables(dry_run=False)")
    
    print(f"\n{'=' * 100}\n")


def vacuum_all_pipeline_tables(retention_hours: int = None, dry_run: bool = True):
    """
    Batch VACUUM for all pipeline tables with safety controls.
    
    ⚠️ WARNING: When dry_run=False, this PERMANENTLY deletes old versions across ALL tables!
    
    Args:
        retention_hours: Retention period (default: from MaintenanceConfig)
        dry_run: If True, only preview (DEFAULT: True for safety)
    """
    if retention_hours is None:
        retention_hours = MaintenanceConfig.VACUUM_RETENTION_HOURS
    
    start_time = datetime.now()
    
    print(f"\n{'=' * 100}")
    print(" " * 30 + "VACUUM ALL PIPELINE TABLES")
    if dry_run:
        print(" " * 35 + "[DRY RUN MODE]")
    else:
        print(" " * 28 + "⚠️  [DESTRUCTIVE MODE] ⚠️")
    print(f"{'=' * 100}\n")
    
    if not dry_run:
        print("⚠️⚠️⚠️  WARNING: You are about to PERMANENTLY delete old file versions! ⚠️⚠️⚠️")
        print("   • This operation CANNOT be undone")
        print(f"   • Time travel will be limited to the past {retention_hours // 24} days")
        print("   • Ensure you have reviewed the impact analysis first\n")
    
    print(f"Processing {len(TableNames.get_all_qualified_tables())} tables...\n")
    
    success_count = 0
    error_count = 0
    
    for table in TableNames.get_all_qualified_tables():
        try:
            vacuum_table(table, retention_hours=retention_hours, dry_run=dry_run)
            success_count += 1
        except Exception as e:
            print(f"✗ Failed to vacuum {table}: {e}\n")
            error_count += 1
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"\n{'=' * 100}")
    print(f"✓ VACUUM {'preview' if dry_run else 'execution'} completed in {duration:.1f} seconds")
    print(f"   Successful: {success_count}")
    print(f"   Errors: {error_count}")
    
    if dry_run:
        print(f"\n💡 To execute: vacuum_all_pipeline_tables(retention_hours={retention_hours}, dry_run=False)")
    else:
        print(f"\n⚠️  Old file versions have been PERMANENTLY deleted")
        print(f"   Time travel is now limited to the past {retention_hours // 24} days")
    
    print(f"{'=' * 100}\n")


def vacuum_all_tables(table_names: List[str], retention_hours: int = 720, dry_run: bool = True):
    """
    Batch VACUUM for a custom list of tables.
    
    ⚠️ Use vacuum_all_pipeline_tables() for all pipeline tables (recommended).
    
    Args:
        table_names: List of fully qualified table names
        retention_hours: Retention period in hours
        dry_run: If True, only preview (DEFAULT: True)
    """
    print(f"\n{'=' * 80}")
    print(f"VACUUM {len(table_names)} tables (retention: {retention_hours} hours)")
    if dry_run:
        print("[DRY RUN MODE]")
    print(f"{'=' * 80}\n")
    
    for table in table_names:
        vacuum_table(table, retention_hours, dry_run)


# Best practices and warnings
print("""
================================================================================
                    VACUUM UTILITIES LOADED
================================================================================

⚠️  CRITICAL: VACUUM permanently deletes old file versions!

Recommended Workflow:
  1. show_vacuum_impact_analysis()      # Analyze all tables
  2. show_vacuum_preview(table_name)    # Preview specific table
  3. vacuum_all_pipeline_tables(dry_run=True)   # Dry run
  4. vacuum_all_pipeline_tables(dry_run=False)  # Execute if satisfied

Best Practices:
  • Always run impact analysis first
  • Use 30-day retention (720 hours) for production
  • Run VACUUM monthly, not weekly
  • Never vacuum during critical time windows
  • Document before/after state for audit trail
  
Safety:
  • All functions default to dry_run=True
  • Retention default: 30 days (from MaintenanceConfig)
  • Preview functions show impact before execution
  
================================================================================
""")
