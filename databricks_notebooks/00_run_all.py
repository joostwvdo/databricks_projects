# Databricks notebook source
# MAGIC %md
# MAGIC # Run All Pipeline
# MAGIC Master notebook om de volledige pipeline te runnen.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

# MAGIC %run ./01_setup_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze Layer - Ingest

# COMMAND ----------

# MAGIC %run ./02_bronze_ingest_matches

# COMMAND ----------

# MAGIC %run ./03_bronze_ingest_teams

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver Layer - SCD-2 Transform

# COMMAND ----------

# MAGIC %run ./04_silver_scd2_processor

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold Layer - Aggregate

# COMMAND ----------

# MAGIC %run ./07_gold_fact_matches

# COMMAND ----------

# MAGIC %run ./08_gold_dim_teams

# COMMAND ----------

# MAGIC %run ./09_gold_head_to_head

# COMMAND ----------

# MAGIC %run ./10_gold_team_form

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data Quality Report

# COMMAND ----------

# MAGIC %run ./11_dbt_data_quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Samenvatting

# COMMAND ----------

CATALOG = "football_data"

print("=" * 60)
print("🎉 PIPELINE COMPLEET!")
print("=" * 60)
print()
print("📊 Aangemaakte tabellen:")
print()

for schema in ["bronze", "silver", "gold"]:
    print(f"\n{schema.upper()}:")
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").collect()
    for t in tables:
        count = spark.table(f"{CATALOG}.{schema}.{t.tableName}").count()
        print(f"  ✅ {t.tableName}: {count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quick Stats

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     competition_name,
# MAGIC     COUNT(*) as wedstrijden,
# MAGIC     ROUND(AVG(total_goals), 2) as gem_goals
# MAGIC FROM football_data.gold.fct_match_results
# MAGIC GROUP BY competition_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 5 per competitie
# MAGIC SELECT * FROM (
# MAGIC     SELECT 
# MAGIC         competition_name,
# MAGIC         team_name,
# MAGIC         current_position,
# MAGIC         current_points,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY competition_name ORDER BY current_position) as rn
# MAGIC     FROM football_data.gold.dim_teams
# MAGIC ) WHERE rn <= 5
# MAGIC ORDER BY competition_name, current_position
