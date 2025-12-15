# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Team Form Analysis
# MAGIC Analyse van de laatste wedstrijden per team.

# COMMAND ----------

CATALOG_NAME = "football_data"
GOLD_SCHEMA = "gold"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_matches = spark.table(f"{GOLD_SCHEMA}.fct_match_results")

# Home perspective
df_home = df_matches.select(
    F.col("match_date"),
    F.col("home_team_name").alias("team"),
    F.col("competition_name"),
    F.col("home_goals").alias("goals_scored"),
    F.col("away_goals").alias("goals_conceded"),
    F.when(F.col("match_result") == "HOME", "W")
     .when(F.col("match_result") == "DRAW", "D")
     .otherwise("L").alias("result"),
    F.col("home_points").alias("points")
)

# Away perspective
df_away = df_matches.select(
    F.col("match_date"),
    F.col("away_team_name").alias("team"),
    F.col("competition_name"),
    F.col("away_goals").alias("goals_scored"),
    F.col("home_goals").alias("goals_conceded"),
    F.when(F.col("match_result") == "AWAY", "W")
     .when(F.col("match_result") == "DRAW", "D")
     .otherwise("L").alias("result"),
    F.col("away_points").alias("points")
)

df_all = df_home.union(df_away)

# COMMAND ----------

# Window voor laatste 5 wedstrijden
w = Window.partitionBy("team", "competition_name").orderBy(F.desc("match_date"))

df_ranked = df_all.withColumn("match_rank", F.row_number().over(w))

# Filter laatste 5
df_last5 = df_ranked.filter("match_rank <= 5")

# Aggregate
df_form = df_last5.groupBy("team", "competition_name").agg(
    F.count("*").alias("matches_in_form"),
    F.sum("points").alias("form_points"),
    F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("form_wins"),
    F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("form_draws"),
    F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("form_losses"),
    F.sum("goals_scored").alias("form_goals_scored"),
    F.sum("goals_conceded").alias("form_goals_conceded"),
    F.concat_ws("", F.collect_list("result")).alias("form_string")
).withColumn(
    "form_ppg", F.round(F.col("form_points") / F.col("matches_in_form"), 2)
).withColumn(
    "form_rating",
    F.when(F.col("form_points") >= 13, "🔥 Excellent")
     .when(F.col("form_points") >= 10, "✅ Good")
     .when(F.col("form_points") >= 7, "➡️ Average")
     .when(F.col("form_points") >= 4, "⚠️ Poor")
     .otherwise("❌ Very Poor")
)

# COMMAND ----------

table_name = f"{GOLD_SCHEMA}.agg_team_form"
df_form.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✅ {df_form.count()} team form records opgeslagen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vorm per Competitie

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    team,
    form_string,
    form_points,
    form_ppg,
    form_goals_scored || '-' || form_goals_conceded as goals,
    form_rating
FROM {table_name}
WHERE competition_name = 'Eredivisie'
ORDER BY form_points DESC
"""))

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    team,
    form_string,
    form_points,
    form_rating
FROM {table_name}
WHERE competition_name = 'Premier League'
ORDER BY form_points DESC
LIMIT 10
"""))
