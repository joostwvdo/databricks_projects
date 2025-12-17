# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer - Generic SCD-2 Processor
# MAGIC 
# MAGIC Generieke routine voor het verwerken van meerdere tabellen naar Silver layer met SCD Type 2.
# MAGIC 
# MAGIC **SCD Type 2 betekent:**
# MAGIC - Nieuwe records krijgen `is_current = true` en `valid_from = now()`
# MAGIC - Bij wijzigingen wordt het oude record afgesloten (`is_current = false`, `valid_to = now()`)
# MAGIC - Volledige historie wordt bewaard

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuratie

# COMMAND ----------

CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SCD-2 Processor Class

# COMMAND ----------

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.window import Window
from typing import List, Dict, Optional
from datetime import datetime

class SCD2Processor:
    """
    Generieke SCD Type 2 processor voor Delta Lake tabellen.
    
    Gebruik:
        processor = SCD2Processor(spark, catalog, silver_schema)
        processor.process_table(
            source_df=bronze_df,
            target_table="stg_matches",
            business_keys=["match_id"],
            hash_columns=["status", "home_score", "away_score"],
            transform_func=my_transform_function
        )
    """
    
    def __init__(self, spark_session, catalog: str, silver_schema: str):
        self.spark = spark_session
        self.catalog = catalog
        self.silver_schema = silver_schema
    
    def _get_full_table_name(self, table_name: str) -> str:
        return f"{self.catalog}.{self.silver_schema}.{table_name}"
    
    def _table_exists(self, table_name: str) -> bool:
        """Check of de tabel bestaat."""
        full_name = self._get_full_table_name(table_name)
        try:
            self.spark.table(full_name)
            return True
        except:
            return False
    
    def _add_scd2_columns(self, df: DataFrame, hash_columns: List[str]) -> DataFrame:
        """Voeg SCD-2 kolommen toe aan DataFrame."""
        # Maak record hash van de opgegeven kolommen
        hash_expr = F.md5(F.concat_ws("|", *[
            F.coalesce(F.col(c).cast("string"), F.lit("")) 
            for c in hash_columns
        ]))
        
        return df.withColumn("record_hash", hash_expr) \
                 .withColumn("valid_from", F.current_timestamp()) \
                 .withColumn("valid_to", F.lit(None).cast("timestamp")) \
                 .withColumn("is_current", F.lit(True)) \
                 .withColumn("transformed_at", F.current_timestamp())
    
    def process_table(
        self,
        source_df: DataFrame,
        target_table: str,
        business_keys: List[str],
        hash_columns: List[str],
        transform_func: Optional[callable] = None
    ) -> Dict:
        """
        Verwerk een bron DataFrame naar Silver layer met SCD-2.
        
        Args:
            source_df: Bron DataFrame (uit Bronze)
            target_table: Naam van de Silver tabel
            business_keys: Kolommen die een record uniek identificeren
            hash_columns: Kolommen om wijzigingen te detecteren
            transform_func: Optionele transformatie functie
        
        Returns:
            Dict met statistieken
        """
        full_table_name = self._get_full_table_name(target_table)
        stats = {
            "table": target_table,
            "source_count": source_df.count(),
            "new_records": 0,
            "updated_records": 0,
            "unchanged_records": 0
        }
        
        # Pas optionele transformatie toe
        if transform_func:
            df_transformed = transform_func(source_df)
        else:
            df_transformed = source_df
        
        # Voeg SCD-2 kolommen toe
        df_new = self._add_scd2_columns(df_transformed, hash_columns)
        
        # Check of tabel bestaat
        if not self._table_exists(target_table):
            # Eerste keer: schrijf alles
            df_new.write \
                .format("delta") \
                .mode("overwrite") \
                .option("overwriteSchema", "true") \
                .saveAsTable(full_table_name)
            
            stats["new_records"] = stats["source_count"]
            print(f"✅ Nieuwe tabel {target_table} aangemaakt met {stats['new_records']} records")
            return stats
        
        # Tabel bestaat: voer SCD-2 merge uit
        df_existing = self.spark.table(full_table_name).filter("is_current = true")
        
        # Join op business keys
        join_condition = " AND ".join([f"new.{k} = existing.{k}" for k in business_keys])
        
        # Vergelijk hashes
        df_comparison = df_new.alias("new").join(
            df_existing.alias("existing"),
            on=business_keys,
            how="full_outer"
        ).select(
            *[F.coalesce(F.col(f"new.{k}"), F.col(f"existing.{k}")).alias(k) for k in business_keys],
            F.col("new.record_hash").alias("new_hash"),
            F.col("existing.record_hash").alias("existing_hash"),
            F.when(F.col("existing.record_hash").isNull(), "INSERT")
             .when(F.col("new.record_hash").isNull(), "DELETE")
             .when(F.col("new.record_hash") != F.col("existing.record_hash"), "UPDATE")
             .otherwise("UNCHANGED").alias("_action")
        )
        
        # Tel acties
        action_counts = df_comparison.groupBy("_action").count().collect()
        for row in action_counts:
            if row["_action"] == "INSERT":
                stats["new_records"] = row["count"]
            elif row["_action"] == "UPDATE":
                stats["updated_records"] = row["count"]
            elif row["_action"] == "UNCHANGED":
                stats["unchanged_records"] = row["count"]
        
        # Voer MERGE uit met Delta Lake
        from delta.tables import DeltaTable
        
        delta_table = DeltaTable.forName(self.spark, full_table_name)
        
        # Sluit oude records af (voor updates)
        keys_to_update = df_comparison.filter("_action = 'UPDATE'").select(business_keys)
        if keys_to_update.count() > 0:
            update_condition = " AND ".join([
                f"target.{k} = updates.{k}" for k in business_keys
            ])
            
            delta_table.alias("target").merge(
                keys_to_update.alias("updates"),
                f"{update_condition} AND target.is_current = true"
            ).whenMatchedUpdate(set={
                "valid_to": F.current_timestamp(),
                "is_current": F.lit(False)
            }).execute()
        
        # Insert nieuwe en gewijzigde records
        df_to_insert = df_new.join(
            df_comparison.filter("_action IN ('INSERT', 'UPDATE')").select(business_keys),
            on=business_keys,
            how="inner"
        )
        
        if df_to_insert.count() > 0:
            df_to_insert.write \
                .format("delta") \
                .mode("append") \
                .saveAsTable(full_table_name)
        
        print(f"✅ {target_table}: {stats['new_records']} nieuw, {stats['updated_records']} updated, {stats['unchanged_records']} ongewijzigd")
        return stats

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformatie Functies

# COMMAND ----------

def transform_matches(df: DataFrame) -> DataFrame:
    """Transformeer matches van Bronze naar Silver formaat."""
    return df.select(
        F.col("match_id"),
        F.col("competition_code"),
        F.col("competition_name"),
        F.col("season"),
        F.col("matchday"),
        F.to_timestamp("match_date").alias("match_datetime"),
        F.to_date("match_date").alias("match_date"),
        F.col("status").alias("match_status"),
        F.col("home_team_id"),
        F.col("home_team_name"),
        F.col("away_team_id"),
        F.col("away_team_name"),
        F.col("home_score").cast("int").alias("home_goals"),
        F.col("away_score").cast("int").alias("away_goals"),
        F.when(F.col("status") == "FINISHED", True).otherwise(False).alias("is_finished"),
        F.when(F.col("home_score") > F.col("away_score"), "HOME")
         .when(F.col("home_score") < F.col("away_score"), "AWAY")
         .when(F.col("home_score") == F.col("away_score"), "DRAW")
         .otherwise(None).alias("match_result"),
        (F.col("home_score") - F.col("away_score")).alias("goal_difference"),
        (F.col("home_score") + F.col("away_score")).alias("total_goals")
    )

def transform_teams(df: DataFrame) -> DataFrame:
    """Transformeer teams van Bronze naar Silver formaat."""
    return df.select(
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
        F.col("competition_name")
    ).dropDuplicates(["team_id", "competition_code"])

def transform_standings(df: DataFrame) -> DataFrame:
    """Transformeer standings van Bronze naar Silver formaat."""
    return df.select(
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
        F.round(F.col("points") / F.col("played_games"), 2).alias("points_per_game"),
        F.when(F.col("position") <= 4, "Champions League")
         .when(F.col("position") <= 6, "Europa League")
         .when(F.col("position") <= 7, "Conference League")
         .when(F.col("position") >= 16, "Relegation")
         .otherwise("Mid-table").alias("table_zone")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tabel Configuratie

# COMMAND ----------

# Configuratie voor alle Silver tabellen
TABLE_CONFIGS = [
    {
        "source_table": "matches",
        "target_table": "stg_matches",
        "business_keys": ["match_id"],
        "hash_columns": ["status", "home_score", "away_score"],
        "transform_func": transform_matches
    },
    {
        "source_table": "teams",
        "target_table": "stg_teams",
        "business_keys": ["team_id", "competition_code"],
        "hash_columns": ["team_name", "venue"],
        "transform_func": transform_teams
    },
    {
        "source_table": "standings",
        "target_table": "stg_standings",
        "business_keys": ["team_id", "competition_code"],
        "hash_columns": ["position", "points", "goals_for", "goals_against"],
        "transform_func": transform_standings
    }
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verwerk Alle Tabellen

# COMMAND ----------

# Initialiseer processor
processor = SCD2Processor(spark, CATALOG_NAME, SILVER_SCHEMA)

# Verwerk alle tabellen
all_stats = []
print("=" * 60)
print("🔄 START SILVER LAYER SCD-2 PROCESSING")
print("=" * 60)
print()

for config in TABLE_CONFIGS:
    print(f"📊 Verwerken: {config['source_table']} → {config['target_table']}")
    
    # Lees bron data
    source_df = spark.table(f"{BRONZE_SCHEMA}.{config['source_table']}")
    
    # Verwerk met SCD-2
    stats = processor.process_table(
        source_df=source_df,
        target_table=config["target_table"],
        business_keys=config["business_keys"],
        hash_columns=config["hash_columns"],
        transform_func=config["transform_func"]
    )
    
    all_stats.append(stats)
    print()

print("=" * 60)
print("✅ SILVER LAYER PROCESSING COMPLEET")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Samenvatting

# COMMAND ----------

# Toon statistieken
from pyspark.sql import Row

stats_df = spark.createDataFrame([Row(**s) for s in all_stats])
display(stats_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificatie - Historie Check

# COMMAND ----------

# Toon dat SCD-2 werkt door alle versies te tonen
display(spark.sql(f"""
SELECT 
    'stg_matches' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records,
    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END) as historical_records
FROM {SILVER_SCHEMA}.stg_matches

UNION ALL

SELECT 
    'stg_teams',
    COUNT(*),
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END)
FROM {SILVER_SCHEMA}.stg_teams

UNION ALL

SELECT 
    'stg_standings',
    COUNT(*),
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END),
    SUM(CASE WHEN NOT is_current THEN 1 ELSE 0 END)
FROM {SILVER_SCHEMA}.stg_standings
"""))
