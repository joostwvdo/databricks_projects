# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Data Quality Report
# MAGIC 
# MAGIC Automatisch data kwaliteitsrapport voor Silver en Gold layers.
# MAGIC 
# MAGIC **Checks:**
# MAGIC - Completeness (nulls, missing values)
# MAGIC - Uniqueness (duplicaten)
# MAGIC - Validity (data types, ranges)
# MAGIC - Consistency (referential integrity)
# MAGIC - Timeliness (data freshness)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup

# COMMAND ----------

CATALOG_NAME = "football_data"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Data Quality Framework

# COMMAND ----------

class DataQualityChecker:
    """Framework voor data quality checks."""
    
    def __init__(self, spark_session):
        self.spark = spark_session
        self.results = []
    
    def add_result(self, layer: str, table: str, check_type: str, 
                   check_name: str, passed: bool, details: str, 
                   records_checked: int = 0, records_failed: int = 0):
        """Voeg een check resultaat toe."""
        self.results.append({
            "layer": layer,
            "table": table,
            "check_type": check_type,
            "check_name": check_name,
            "status": "✅ PASS" if passed else "❌ FAIL",
            "passed": passed,
            "details": details,
            "records_checked": records_checked,
            "records_failed": records_failed,
            "check_timestamp": datetime.now().isoformat()
        })
    
    def check_not_null(self, df, layer: str, table: str, columns: list):
        """Check op NULL waarden."""
        total = df.count()
        for col in columns:
            null_count = df.filter(F.col(col).isNull()).count()
            passed = null_count == 0
            self.add_result(
                layer, table, "Completeness", f"NOT NULL: {col}",
                passed, f"{null_count} nulls gevonden",
                total, null_count
            )
    
    def check_unique(self, df, layer: str, table: str, columns: list):
        """Check op unieke waarden."""
        total = df.count()
        key_cols = columns if isinstance(columns, list) else [columns]
        
        duplicates = df.groupBy(key_cols).count().filter("count > 1")
        dup_count = duplicates.count()
        
        passed = dup_count == 0
        self.add_result(
            layer, table, "Uniqueness", f"UNIQUE: {', '.join(key_cols)}",
            passed, f"{dup_count} duplicate keys gevonden",
            total, dup_count
        )
    
    def check_referential_integrity(self, df_child, df_parent, 
                                     layer: str, table: str,
                                     child_col: str, parent_col: str,
                                     parent_table: str):
        """Check referentiële integriteit."""
        total = df_child.count()
        
        orphans = df_child.join(
            df_parent.select(F.col(parent_col).alias("_parent_key")),
            df_child[child_col] == F.col("_parent_key"),
            "left_anti"
        ).count()
        
        passed = orphans == 0
        self.add_result(
            layer, table, "Consistency", 
            f"FK: {child_col} → {parent_table}.{parent_col}",
            passed, f"{orphans} orphan records",
            total, orphans
        )
    
    def check_valid_values(self, df, layer: str, table: str, 
                           column: str, valid_values: list):
        """Check of waarden in toegestane lijst zitten."""
        total = df.filter(F.col(column).isNotNull()).count()
        
        invalid = df.filter(
            (F.col(column).isNotNull()) & 
            (~F.col(column).isin(valid_values))
        ).count()
        
        passed = invalid == 0
        self.add_result(
            layer, table, "Validity", f"VALID VALUES: {column}",
            passed, f"{invalid} invalid values (allowed: {valid_values})",
            total, invalid
        )
    
    def check_range(self, df, layer: str, table: str,
                    column: str, min_val=None, max_val=None):
        """Check of numerieke waarden binnen range zijn."""
        total = df.filter(F.col(column).isNotNull()).count()
        
        conditions = []
        if min_val is not None:
            conditions.append(F.col(column) < min_val)
        if max_val is not None:
            conditions.append(F.col(column) > max_val)
        
        if conditions:
            from functools import reduce
            from operator import or_
            invalid = df.filter(reduce(or_, conditions)).count()
        else:
            invalid = 0
        
        passed = invalid == 0
        range_str = f"[{min_val}, {max_val}]"
        self.add_result(
            layer, table, "Validity", f"RANGE: {column} in {range_str}",
            passed, f"{invalid} out of range",
            total, invalid
        )
    
    def check_freshness(self, df, layer: str, table: str,
                        timestamp_col: str, max_age_hours: int = 24):
        """Check data freshness."""
        max_ts = df.agg(F.max(timestamp_col)).collect()[0][0]
        
        if max_ts:
            age_hours = (datetime.now() - max_ts).total_seconds() / 3600
            passed = age_hours <= max_age_hours
            details = f"Laatste update: {max_ts}, {age_hours:.1f} uur geleden"
        else:
            passed = False
            details = "Geen timestamp data"
        
        self.add_result(
            layer, table, "Timeliness", f"FRESHNESS: {timestamp_col}",
            passed, details
        )
    
    def check_scd2_integrity(self, df, layer: str, table: str, business_key: str):
        """Check SCD-2 integriteit - max 1 current record per key."""
        current_df = df.filter("is_current = true")
        
        # Check: max 1 current per business key
        multi_current = current_df.groupBy(business_key).count().filter("count > 1").count()
        
        passed = multi_current == 0
        self.add_result(
            layer, table, "SCD2 Integrity", f"SINGLE CURRENT: {business_key}",
            passed, f"{multi_current} keys met meerdere current records"
        )
        
        # Check: valid_from <= valid_to voor gesloten records
        closed_invalid = df.filter(
            (F.col("is_current") == False) & 
            (F.col("valid_to").isNull() | (F.col("valid_from") > F.col("valid_to")))
        ).count()
        
        passed = closed_invalid == 0
        self.add_result(
            layer, table, "SCD2 Integrity", "VALID DATES: closed records",
            passed, f"{closed_invalid} records met ongeldige valid_from/valid_to"
        )
    
    def get_results_df(self):
        """Return resultaten als DataFrame."""
        from pyspark.sql import Row
        return self.spark.createDataFrame([Row(**r) for r in self.results])
    
    def get_summary(self):
        """Return samenvatting."""
        total = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        failed = total - passed
        
        return {
            "total_checks": total,
            "passed": passed,
            "failed": failed,
            "pass_rate": f"{100*passed/total:.1f}%" if total > 0 else "N/A"
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Silver Layer Checks

# COMMAND ----------

dq = DataQualityChecker(spark)

print("=" * 60)
print("🔍 SILVER LAYER DATA QUALITY CHECKS")
print("=" * 60)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 stg_matches

# COMMAND ----------

df_matches = spark.table(f"{SILVER_SCHEMA}.stg_matches")
print(f"📊 stg_matches: {df_matches.count()} records")

# Completeness
dq.check_not_null(df_matches, "Silver", "stg_matches", 
                  ["match_id", "competition_code", "home_team_id", "away_team_id"])

# Uniqueness (voor current records)
df_current = df_matches.filter("is_current = true")
dq.check_unique(df_current, "Silver", "stg_matches", ["match_id"])

# Validity
dq.check_valid_values(df_matches, "Silver", "stg_matches", "match_result", 
                      ["HOME", "AWAY", "DRAW", None])
dq.check_valid_values(df_matches, "Silver", "stg_matches", "competition_code",
                      ["DED", "PL", "PD", "SA"])

# Range checks
dq.check_range(df_matches, "Silver", "stg_matches", "home_goals", min_val=0, max_val=20)
dq.check_range(df_matches, "Silver", "stg_matches", "away_goals", min_val=0, max_val=20)

# SCD-2 integrity
dq.check_scd2_integrity(df_matches, "Silver", "stg_matches", "match_id")

# Freshness
dq.check_freshness(df_matches, "Silver", "stg_matches", "transformed_at", max_age_hours=48)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 stg_teams

# COMMAND ----------

df_teams = spark.table(f"{SILVER_SCHEMA}.stg_teams")
print(f"📊 stg_teams: {df_teams.count()} records")

dq.check_not_null(df_teams, "Silver", "stg_teams", ["team_id", "team_name"])
dq.check_unique(df_teams.filter("is_current = true"), "Silver", "stg_teams", 
                ["team_id", "competition_code"])
dq.check_scd2_integrity(df_teams, "Silver", "stg_teams", "team_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 stg_standings

# COMMAND ----------

df_standings = spark.table(f"{SILVER_SCHEMA}.stg_standings")
print(f"📊 stg_standings: {df_standings.count()} records")

dq.check_not_null(df_standings, "Silver", "stg_standings", 
                  ["team_id", "position", "points"])
dq.check_range(df_standings, "Silver", "stg_standings", "position", min_val=1, max_val=20)
dq.check_range(df_standings, "Silver", "stg_standings", "points", min_val=0, max_val=150)
dq.check_scd2_integrity(df_standings, "Silver", "stg_standings", "team_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Gold Layer Checks

# COMMAND ----------

print()
print("=" * 60)
print("🔍 GOLD LAYER DATA QUALITY CHECKS")
print("=" * 60)
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 fct_match_results

# COMMAND ----------

df_fact = spark.table(f"{GOLD_SCHEMA}.fct_match_results")
print(f"📊 fct_match_results: {df_fact.count()} records")

dq.check_not_null(df_fact, "Gold", "fct_match_results", 
                  ["match_id", "match_date", "home_team_id", "away_team_id"])
dq.check_unique(df_fact, "Gold", "fct_match_results", ["match_id"])

# Points validation
dq.check_valid_values(df_fact, "Gold", "fct_match_results", "home_points", [0, 1, 3])
dq.check_valid_values(df_fact, "Gold", "fct_match_results", "away_points", [0, 1, 3])

# Consistency: home_points + away_points moet altijd 3 zijn (win/loss) of 2 (draw)
invalid_points = df_fact.filter(
    ~((F.col("home_points") + F.col("away_points")).isin([2, 3]))
).count()
dq.add_result("Gold", "fct_match_results", "Consistency", 
              "POINTS SUM: home + away = 2 or 3",
              invalid_points == 0, f"{invalid_points} invalid",
              df_fact.count(), invalid_points)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 dim_teams

# COMMAND ----------

df_dim = spark.table(f"{GOLD_SCHEMA}.dim_teams")
print(f"📊 dim_teams: {df_dim.count()} records")

dq.check_not_null(df_dim, "Gold", "dim_teams", ["team_id", "team_name"])
dq.check_unique(df_dim, "Gold", "dim_teams", ["team_id", "competition_code"])

# Referential integrity met fact table
dq.check_referential_integrity(
    df_fact, df_dim, "Gold", "fct_match_results",
    "home_team_id", "team_id", "dim_teams"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 agg_head_to_head

# COMMAND ----------

try:
    df_h2h = spark.table(f"{GOLD_SCHEMA}.agg_head_to_head")
    print(f"📊 agg_head_to_head: {df_h2h.count()} records")
    
    dq.check_not_null(df_h2h, "Gold", "agg_head_to_head", ["team", "opponent"])
    
    # Check: wins + draws + losses = matches_played
    invalid_record = df_h2h.filter(
        F.col("wins") + F.col("draws") + F.col("losses") != F.col("matches_played")
    ).count()
    dq.add_result("Gold", "agg_head_to_head", "Consistency",
                  "RECORD SUM: W+D+L = matches",
                  invalid_record == 0, f"{invalid_record} invalid")
except:
    print("⚠️ agg_head_to_head niet gevonden")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 agg_team_form

# COMMAND ----------

try:
    df_form = spark.table(f"{GOLD_SCHEMA}.agg_team_form")
    print(f"📊 agg_team_form: {df_form.count()} records")
    
    dq.check_not_null(df_form, "Gold", "agg_team_form", ["team", "form_points"])
    dq.check_range(df_form, "Gold", "agg_team_form", "form_points", min_val=0, max_val=15)
    dq.check_range(df_form, "Gold", "agg_team_form", "matches_in_form", min_val=1, max_val=5)
except:
    print("⚠️ agg_team_form niet gevonden")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Results Report

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Summary

# COMMAND ----------

summary = dq.get_summary()
print("=" * 60)
print("📋 DATA QUALITY SUMMARY")
print("=" * 60)
print(f"Total Checks:  {summary['total_checks']}")
print(f"Passed:        {summary['passed']} ✅")
print(f"Failed:        {summary['failed']} ❌")
print(f"Pass Rate:     {summary['pass_rate']}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Detailed Results

# COMMAND ----------

df_results = dq.get_results_df()
display(df_results.orderBy("layer", "table", "check_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Failed Checks Only

# COMMAND ----------

df_failed = df_results.filter("passed = false")
if df_failed.count() > 0:
    print("❌ FAILED CHECKS:")
    display(df_failed)
else:
    print("✅ Alle checks geslaagd!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Save Report to Delta

# COMMAND ----------

# Sla rapport op voor historische tracking
report_table = f"{GOLD_SCHEMA}.data_quality_history"

df_results.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(report_table)

print(f"✅ Rapport opgeslagen in {report_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Quality Trend

# COMMAND ----------

display(spark.sql(f"""
SELECT 
    DATE(check_timestamp) as check_date,
    layer,
    COUNT(*) as total_checks,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN NOT passed THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN passed THEN 1 ELSE 0 END) / COUNT(*), 1) as pass_rate
FROM {report_table}
GROUP BY DATE(check_timestamp), layer
ORDER BY check_date DESC, layer
"""))
