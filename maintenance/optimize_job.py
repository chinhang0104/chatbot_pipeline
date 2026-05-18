# Databricks notebook source
# MAGIC %md
# MAGIC # Table Maintenance Job
# MAGIC
# MAGIC This notebook runs scheduled maintenance tasks for all pipeline tables.
# MAGIC
# MAGIC **What it does:**
# MAGIC * Optimizes all tables using OPTIMIZE command (liquid clustering for Gold tables, Z-ordering for Bronze/Silver)
# MAGIC * Configures recommended table properties for performance
# MAGIC * Displays table health metrics and optimization recommendations
# MAGIC
# MAGIC **Scheduling:**
# MAGIC This notebook is scheduled to run regularly to maintain optimal table performance. Job ID: 961719883410776
# MAGIC
# MAGIC **For VACUUM operations:** See the separate `vacuum_job` script in the utilities folder.

# COMMAND ----------

# Add the pipeline root to the Python path
import sys
sys.path.insert(0, "/Workspace/Users/chinhang0104@gmail.com/chatbot_pipeline")

# Import maintenance functions from utilities/maintenance.py
from utilities.maintenance import (
    run_all_maintenance,
    optimize_all_tables,
    configure_all_tables,
    analyze_all_tables,
    show_table_health,
    show_optimization_recommendations
)

# COMMAND ----------

# Run complete maintenance workflow
# This includes:
# - OPTIMIZE (liquid clustering for Gold, Z-ordering for Bronze/Silver)
# - Table property configuration
# - Optional: Table analysis (disabled by default for performance)

run_all_maintenance(
    include_analysis=False,  # Set to True to include table statistics computation
    dry_run=False            # Set to True to preview without executing
)

# COMMAND ----------

# Show health metrics for all pipeline tables
# Displays: table sizes, file counts, versions, and potential issues

show_table_health()

# COMMAND ----------

# Display current optimization strategy for all tables
# Shows which tables use liquid clustering vs Z-ordering

show_optimization_recommendations()