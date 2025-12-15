# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Transform Matches
# MAGIC Transformeert ruwe wedstrijddata naar gecleande silver layer met SCD Type 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuratie

# COMMAND ----------

CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lees Bronze Data

# COMMAND ----------

df_bronze = spark.table(f"{BRONZE_SCHEMA}.matches")
print(f"Bronze records: {df_bronze.count()}")
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformaties

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_silver = df_bronze.select(
    # Keys
    F.col("match_id"),
    F.col("competition_code"),
    F.col("competition_name"),
    F.col("season"),
    F.col("matchday"),
    
    # Match info
    F.to_timestamp("match_date").alias("match_datetime"),
    F.to_date("match_date").alias("match_date"),
    F.col("status").alias("match_status"),
    
    # Teams
    F.col("home_team_id"),
    F.col("home_team_name"),
    F.col("away_team_id"),
    F.col("away_team_name"),
    
    # Scores
    F.col("home_score").cast("int").alias("home_goals"),
    F.col("away_score").cast("int").alias("away_goals"),
    
    # Calculated fields
    F.when(F.col("status") == "FINISHED", True).otherwise(False).alias("is_finished"),
    
    F.when(F.col("home_score") > F.col("away_score"), "HOME")
     .when(F.col("home_score") < F.col("away_score"), "AWAY")
     .when(F.col("home_score") == F.col("away_score"), "DRAW")
     .otherwise(None).alias("match_result"),
    
    # Goal difference
    (F.col("home_score") - F.col("away_score")).alias("goal_difference"),
    
    # Total goals
    (F.col("home_score") + F.col("away_score")).alias("total_goals"),
    
    # Record hash for SCD2
    F.md5(F.concat_ws("|",
        F.col("status"),
        F.coalesce(F.col("home_score").cast("string"), F.lit("")),
        F.coalesce(F.col("away_score").cast("string"), F.lit(""))
    )).alias("record_hash"),
    
    # SCD2 fields
    F.current_timestamp().alias("valid_from"),
    F.lit(None).cast("timestamp").alias("valid_to"),
    F.lit(True).alias("is_current"),
    
    # Metadata
    F.current_timestamp().alias("transformed_at")
)

print(f"Silver records: {df_silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Opslaan naar Silver Layer

# COMMAND ----------

table_name = f"{SILVER_SCHEMA}.stg_matches"

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✅ Data opgeslagen in {table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificatie

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    competition_name,
    COUNT(*) as total_matches,
    SUM(CASE WHEN is_finished THEN 1 ELSE 0 END) as finished,
    ROUND(AVG(total_goals), 2) as avg_goals_per_match
FROM {table_name}
GROUP BY competition_name
ORDER BY competition_name
"""))

# COMMAND ----------

# Toon recente wedstrijden
display(spark.sql(f"""
SELECT 
    match_date,
    competition_name,
    home_team_name,
    home_goals,
    away_goals,
    away_team_name,
    match_result
FROM {table_name}
WHERE is_finished = true
ORDER BY match_date DESC
LIMIT 20
"""))

# COMMAND ----------

print("🎉 Silver matches transformation compleet!")
