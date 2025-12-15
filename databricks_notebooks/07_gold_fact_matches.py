# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Match Results Fact Table
# MAGIC Maakt de fct_match_results tabel met volledige wedstrijdanalyse.

# COMMAND ----------

CATALOG_NAME = "football_data"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fact Table - Match Results

# COMMAND ----------

from pyspark.sql import functions as F

df_matches = spark.table(f"{SILVER_SCHEMA}.stg_matches").filter("is_current = true")

df_gold = df_matches.select(
    F.col("match_id"),
    F.col("match_date"),
    F.col("match_datetime"),
    F.col("competition_code"),
    F.col("competition_name"),
    F.col("matchday"),
    F.col("home_team_id"),
    F.col("home_team_name"),
    F.col("away_team_id"),
    F.col("away_team_name"),
    F.col("home_goals"),
    F.col("away_goals"),
    F.col("total_goals"),
    F.col("goal_difference"),
    F.col("match_result"),
    F.col("is_finished"),
    
    # Points
    F.when(F.col("match_result") == "HOME", 3)
     .when(F.col("match_result") == "DRAW", 1)
     .otherwise(0).alias("home_points"),
    
    F.when(F.col("match_result") == "AWAY", 3)
     .when(F.col("match_result") == "DRAW", 1)
     .otherwise(0).alias("away_points"),
    
    # Flags
    F.when(F.col("total_goals") >= 3, True).otherwise(False).alias("is_high_scoring"),
    F.when(F.col("home_goals") == 0, True).otherwise(False).alias("home_clean_sheet"),
    F.when(F.col("away_goals") == 0, True).otherwise(False).alias("away_clean_sheet"),
    
    F.current_timestamp().alias("transformed_at")
).filter("is_finished = true")

# COMMAND ----------

table_name = f"{GOLD_SCHEMA}.fct_match_results"
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✅ {df_gold.count()} wedstrijden opgeslagen in {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificatie

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    competition_name,
    COUNT(*) as matches,
    ROUND(AVG(total_goals), 2) as avg_goals,
    SUM(CASE WHEN match_result = 'HOME' THEN 1 ELSE 0 END) as home_wins,
    SUM(CASE WHEN match_result = 'DRAW' THEN 1 ELSE 0 END) as draws,
    SUM(CASE WHEN match_result = 'AWAY' THEN 1 ELSE 0 END) as away_wins
FROM {table_name}
GROUP BY competition_name
"""))
