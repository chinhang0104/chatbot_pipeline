# Databricks notebook source
# MAGIC %md
# MAGIC # Monthly VACUUM Job
# MAGIC
# MAGIC This notebook performs monthly VACUUM operations on all chatbot pipeline tables to reclaim storage and maintain optimal performance.
# MAGIC
# MAGIC **⚠️ WARNING**: VACUUM permanently deletes old file versions!
# MAGIC
# MAGIC ## Workflow
# MAGIC 1. **Impact Analysis** - Analyze which tables will be affected
# MAGIC 2. **Dry Run** - Preview what would be deleted across all tables
# MAGIC 3. **Execute** - Perform actual VACUUM operation
# MAGIC
# MAGIC ## Configuration
# MAGIC - **Retention**: 30 days (720 hours) - standard for production tables
# MAGIC - **Schedule**: Monthly execution via Job ID 666380740792614
# MAGIC - **Tables**: All tables in main.chat_history catalog
# MAGIC
# MAGIC ## Safety Features
# MAGIC - Defaults to dry_run mode for safety
# MAGIC - Detailed logging and impact analysis
# MAGIC - Retention period enforced from MaintenanceConfig

# COMMAND ----------

# Load VACUUM utilities from vacuum_tables.py
%run /Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline/utilities/vacuum_tables

# COMMAND ----------

# Analyze which tables will be affected by VACUUM
# Shows version counts, oldest versions, and impact summary
print("Step 1: Analyzing VACUUM impact across all pipeline tables...\n")

show_vacuum_impact_analysis(retention_hours=720)

print("\n✓ Impact analysis complete")
print("\nReview the tables marked with ⚠️ above.")
print("These tables have versions older than 30 days that will be permanently deleted.")

# COMMAND ----------

# Preview what would be deleted (dry run)
print("Step 2: Running VACUUM dry run across all tables...\n")
print("This shows what WOULD be deleted without actually deleting anything.\n")

vacuum_all_pipeline_tables(retention_hours=720, dry_run=True)

print("\n✓ Dry run complete")
print("\nReview the preview above before proceeding to execution.")

# COMMAND ----------

# Execute VACUUM operation (DESTRUCTIVE - permanently deletes old versions)
print("Step 3: Executing VACUUM operation...\n")
print("⚠️⚠️⚠️ This will PERMANENTLY delete old file versions! ⚠️⚠️⚠️\n")

# Execute VACUUM with 30-day retention
vacuum_all_pipeline_tables(retention_hours=720, dry_run=False)

print("\n" + "="*100)
print("✓ Monthly VACUUM job completed successfully")
print("="*100)
print(f"\nExecution time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Retention period: 30 days (720 hours)")
print(f"Time travel is now limited to the past 30 days for all tables.")
print("\nStorage has been reclaimed by removing old file versions.")
