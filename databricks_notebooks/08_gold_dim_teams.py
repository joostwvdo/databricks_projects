# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Team Dimension
# MAGIC Maakt de dim_teams tabel met team statistieken.

# COMMAND ----------

CATALOG_NAME = "football_data"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

from pyspark.sql import functions as F

# Team base info
df_teams = spark.table(f"{SILVER_SCHEMA}.stg_teams").filter("is_current = true")

# Standings voor huidige positie
df_standings = spark.table(f"{SILVER_SCHEMA}.stg_standings").filter("is_current = true")

# Join
df_dim = df_teams.join(
    df_standings.select(
        "team_id", "competition_code", "position", "points", 
        "wins", "draws", "losses", "goals_for", "goals_against",
        "points_per_game", "table_zone"
    ),
    on=["team_id", "competition_code"],
    how="left"
).select(
    F.col("team_id"),
    F.col("team_name"),
    F.col("team_code"),
    F.col("short_name"),
    F.col("venue"),
    F.col("founded"),
    F.col("club_colors"),
    F.col("competition_code"),
    F.col("competition_name"),
    F.col("position").alias("current_position"),
    F.col("points").alias("current_points"),
    F.col("wins"),
    F.col("draws"),
    F.col("losses"),
    F.col("goals_for"),
    F.col("goals_against"),
    F.col("points_per_game"),
    F.col("table_zone"),
    F.current_timestamp().alias("transformed_at")
)

# COMMAND ----------

table_name = f"{GOLD_SCHEMA}.dim_teams"
df_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
print(f"✅ {df_dim.count()} teams opgeslagen")

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    team_name,
    current_position as pos,
    current_points as pts,
    wins || '-' || draws || '-' || losses as record,
    goals_for || '-' || goals_against as goals,
    table_zone
FROM {table_name}
WHERE competition_name = 'Eredivisie'
ORDER BY current_position
"""))
