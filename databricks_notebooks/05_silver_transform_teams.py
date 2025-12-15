# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Transform Teams
# MAGIC Transformeert ruwe team data naar gecleande silver layer.

# COMMAND ----------

CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lees Bronze Data

# COMMAND ----------

df_bronze = spark.table(f"{BRONZE_SCHEMA}.teams")
print(f"Bronze records: {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformaties

# COMMAND ----------

from pyspark.sql import functions as F

df_silver = df_bronze.select(
    F.col("team_id"),
    F.col("team_name"),
    F.col("short_name"),
    F.col("tla").alias("team_code"),
    F.col("venue"),
    F.col("founded"),
    F.col("club_colors"),
    F.col("address"),
    F.col("website"),
    F.col("competition_code"),
    F.col("competition_name"),
    
    # Record hash for SCD2
    F.md5(F.concat_ws("|",
        F.col("team_name"),
        F.coalesce(F.col("venue"), F.lit("")),
        F.coalesce(F.col("competition_code"), F.lit(""))
    )).alias("record_hash"),
    
    # SCD2 fields
    F.current_timestamp().alias("valid_from"),
    F.lit(None).cast("timestamp").alias("valid_to"),
    F.lit(True).alias("is_current"),
    
    F.current_timestamp().alias("transformed_at")
).dropDuplicates(["team_id", "competition_code"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opslaan

# COMMAND ----------

table_name = f"{SILVER_SCHEMA}.stg_teams"
df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✅ {df_silver.count()} teams opgeslagen in {table_name}")

# COMMAND ----------

display(spark.sql(f"SELECT team_name, team_code, venue, competition_name FROM {table_name} ORDER BY competition_name, team_name"))
