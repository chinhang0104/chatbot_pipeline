{{ config(materialized='table') }}

WITH team_daily AS (
    SELECT 
        team,
        checkpoint_date,
        SUM(checkpoint_count) as team_checkpoint_count,
        SUM(thread_count) as team_thread_count,
        COUNT(DISTINCT user_id) as team_user_count,
        MAX(checkpoint_date) as latest_date
    FROM {{ source('chat_history', 'gold_user_team_metrics') }}
    GROUP BY team, checkpoint_date
)

SELECT * FROM team_daily