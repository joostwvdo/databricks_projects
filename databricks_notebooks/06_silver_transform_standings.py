# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Transform Standings
# MAGIC Transformeert ruwe standings naar gecleande silver layer met SCD Type 2.

# COMMAND ----------

CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

df_bronze = spark.table(f"{BRONZE_SCHEMA}.standings")
print(f"Bronze records: {df_bronze.count()}")

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = df_bronze.select(
    F.col("competition_code"),
    F.col("competition_name"),
    F.col("team_id"),
    F.col("team_name"),
    F.col("position"),
    F.col("played_games").alias("matches_played"),
    F.col("won").alias("wins"),
    F.col("draw").alias("draws"),
    F.col("lost").alias("losses"),
    F.col("points"),
    F.col("goals_for"),
    F.col("goals_against"),
    F.col("goal_difference"),
    
    # Bereken punten per wedstrijd
    F.round(F.col("points") / F.col("played_games"), 2).alias("points_per_game"),
    
    # Zone classificatie
    F.when(F.col("position") <= 4, "Champions League")
     .when(F.col("position") <= 6, "Europa League")
     .when(F.col("position") <= 7, "Conference League")
     .when(F.col("position") >= 16, "Relegation")
     .otherwise("Mid-table").alias("table_zone"),
    
    # Record hash for SCD2
    F.md5(F.concat_ws("|",
        F.col("position").cast("string"),
        F.col("points").cast("string"),
        F.col("goals_for").cast("string"),
        F.col("goals_against").cast("string")
    )).alias("record_hash"),
    
    # SCD2 fields
    F.current_timestamp().alias("valid_from"),
    F.lit(None).cast("timestamp").alias("valid_to"),
    F.lit(True).alias("is_current"),
    
    F.current_timestamp().alias("transformed_at")
)

# COMMAND ----------

table_name = f"{SILVER_SCHEMA}.stg_standings"
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✅ {df_silver.count()} standings opgeslagen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificatie - Standen per competitie

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    position,
    team_name,
    matches_played as P,
    wins as W,
    draws as D,
    losses as L,
    goals_for as GF,
    goals_against as GA,
    goal_difference as GD,
    points as Pts,
    table_zone
FROM {table_name}
WHERE competition_name = 'Eredivisie'
ORDER BY position
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    position,
    team_name,
    points as Pts,
    table_zone
FROM {table_name}
WHERE competition_name = 'Premier League'
ORDER BY position
"""))
