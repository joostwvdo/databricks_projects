# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Head to Head Analysis
# MAGIC Onderlinge resultaten tussen teams.

# COMMAND ----------

CATALOG_NAME = "football_data"
GOLD_SCHEMA = "gold"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Head to Head Berekening

# COMMAND ----------

from pyspark.sql import functions as F

df_matches = spark.table(f"{GOLD_SCHEMA}.fct_match_results")

# Home perspective
df_home = df_matches.select(
    F.col("home_team_name").alias("team"),
    F.col("away_team_name").alias("opponent"),
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
    F.col("away_team_name").alias("team"),
    F.col("home_team_name").alias("opponent"),
    F.col("competition_name"),
    F.col("away_goals").alias("goals_scored"),
    F.col("home_goals").alias("goals_conceded"),
    F.when(F.col("match_result") == "AWAY", "W")
     .when(F.col("match_result") == "DRAW", "D")
     .otherwise("L").alias("result"),
    F.col("away_points").alias("points")
)

# Combine
df_all = df_home.union(df_away)

# Aggregate per team vs opponent
df_h2h = df_all.groupBy("team", "opponent", "competition_name").agg(
    F.count("*").alias("matches_played"),
    F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("wins"),
    F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("draws"),
    F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("losses"),
    F.sum("goals_scored").alias("goals_for"),
    F.sum("goals_conceded").alias("goals_against"),
    F.sum("points").alias("total_points")
).withColumn(
    "goal_difference", F.col("goals_for") - F.col("goals_against")
).withColumn(
    "win_percentage", F.round(100 * F.col("wins") / F.col("matches_played"), 1)
).withColumn(
    "dominance",
    F.when(F.col("wins") > F.col("losses") + 2, "Dominant")
     .when(F.col("wins") > F.col("losses"), "Edge")
     .when(F.col("wins") == F.col("losses"), "Even")
     .otherwise("Underdog")
)

# COMMAND ----------

table_name = f"{GOLD_SCHEMA}.agg_head_to_head"
df_h2h.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✅ {df_h2h.count()} head-to-head records opgeslagen")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Voorbeeld: Ajax vs Feyenoord

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    team,
    opponent,
    matches_played,
    wins || '-' || draws || '-' || losses as record,
    goals_for || '-' || goals_against as goals,
    total_points,
    dominance
FROM {table_name}
WHERE (team = 'AFC Ajax' AND opponent = 'Feyenoord Rotterdam')
   OR (team = 'Feyenoord Rotterdam' AND opponent = 'AFC Ajax')
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Beste H2H Records

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    team,
    opponent,
    competition_name,
    matches_played,
    wins,
    win_percentage,
    dominance
FROM {table_name}
WHERE matches_played >= 2
ORDER BY win_percentage DESC
LIMIT 20
"""))
