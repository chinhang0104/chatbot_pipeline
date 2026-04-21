tables = [
    "main.chat_history.bronze_checkpoints",
    "main.chat_history.silver_checkpoints", 
    "main.chat_history.silver_thread_lookup",
    "main.chat_history.gold_user_team_metrics"
]

for t in tables:
    spark.sql(f"OPTIMIZE {t}")
    spark.sql(f"VACUUM {t} RETAIN 168 HOURS") 714540