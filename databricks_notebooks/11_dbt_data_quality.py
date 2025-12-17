# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality met DBT
# MAGIC Voert DBT tests uit op Silver en Gold layer en genereert een kwaliteitsrapport.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Installeer DBT

# COMMAND ----------

# MAGIC %pip install dbt-core dbt-databricks --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. DBT Project Setup

# COMMAND ----------

import os

# Maak DBT project structuur
dbt_project_path = "/tmp/dbt_football"

os.makedirs(f"{dbt_project_path}/models/silver", exist_ok=True)
os.makedirs(f"{dbt_project_path}/models/gold", exist_ok=True)
os.makedirs(f"{dbt_project_path}/tests", exist_ok=True)

print(f"✅ DBT project folder: {dbt_project_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. DBT Project Configuratie

# COMMAND ----------

# dbt_project.yml
dbt_project_yml = """
name: 'football_quality'
version: '1.0.0'

profile: 'databricks'

model-paths: ["models"]
test-paths: ["tests"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

vars:
  catalog: football_data
"""

with open(f"{dbt_project_path}/dbt_project.yml", "w") as f:
    f.write(dbt_project_yml)

print("✅ dbt_project.yml aangemaakt")

# COMMAND ----------

# profiles.yml - Databricks connectie
profiles_yml = """
databricks:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: football_data
      schema: silver
      host: """ + spark.conf.get("spark.databricks.workspaceUrl") + """
      http_path: /sql/protocolv1/o/0/0000-000000-xxxxxxxx
      token: "{{ env_var('DBT_DATABRICKS_TOKEN') }}"
      threads: 4
"""

# Voor notebook execution gebruiken we een andere methode
print("ℹ️ Profiles worden dynamisch geconfigureerd")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema/Tests Definitie

# COMMAND ----------

# Silver layer schema met tests
silver_schema_yml = """
version: 2

models:
  - name: stg_matches
    description: "Getransformeerde wedstrijden met SCD Type 2"
    columns:
      - name: match_id
        description: "Unieke wedstrijd ID"
        tests:
          - not_null
          - unique:
              config:
                where: "is_current = true"
      
      - name: competition_code
        tests:
          - not_null
          - accepted_values:
              values: ['DED', 'PL', 'PD', 'SA']
      
      - name: match_status
        tests:
          - accepted_values:
              values: ['SCHEDULED', 'TIMED', 'IN_PLAY', 'PAUSED', 'FINISHED', 'SUSPENDED', 'POSTPONED', 'CANCELLED', 'AWARDED']
      
      - name: home_goals
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 20
              config:
                where: "is_finished = true"
      
      - name: is_current
        tests:
          - not_null

  - name: stg_teams
    description: "Team master data met SCD Type 2"
    columns:
      - name: team_id
        tests:
          - not_null
      
      - name: team_name
        tests:
          - not_null

  - name: stg_standings
    description: "Competitie standen met SCD Type 2"
    columns:
      - name: team_id
        tests:
          - not_null
      
      - name: position
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 20
      
      - name: points
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 150
"""

with open(f"{dbt_project_path}/models/silver/schema.yml", "w") as f:
    f.write(silver_schema_yml)

print("✅ Silver schema.yml aangemaakt")

# COMMAND ----------

# Gold layer schema met tests
gold_schema_yml = """
version: 2

models:
  - name: fct_match_results
    description: "Fact table met wedstrijdresultaten"
    columns:
      - name: match_id
        tests:
          - unique
          - not_null
      
      - name: match_date
        tests:
          - not_null
      
      - name: home_points
        tests:
          - accepted_values:
              values: [0, 1, 3]
      
      - name: away_points
        tests:
          - accepted_values:
              values: [0, 1, 3]
      
      - name: total_goals
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 15

  - name: dim_teams
    description: "Team dimensie met statistieken"
    columns:
      - name: team_id
        tests:
          - unique
          - not_null
      
      - name: team_name
        tests:
          - not_null
      
      - name: current_position
        tests:
          - dbt_utils.accepted_range:
              min_value: 1
              max_value: 20
              config:
                where: "current_position is not null"

  - name: agg_head_to_head
    description: "Head to head statistieken"
    columns:
      - name: team
        tests:
          - not_null
      
      - name: opponent
        tests:
          - not_null
      
      - name: matches_played
        tests:
          - dbt_utils.accepted_range:
              min_value: 1

  - name: agg_team_form
    description: "Team vorm analyse"
    columns:
      - name: team
        tests:
          - not_null
      
      - name: form_points
        tests:
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 15
"""

with open(f"{dbt_project_path}/models/gold/schema.yml", "w") as f:
    f.write(gold_schema_yml)

print("✅ Gold schema.yml aangemaakt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Custom Data Quality Tests

# COMMAND ----------

# Custom test: Check referential integrity
custom_tests = """
-- Test: Elke wedstrijd heeft geldige teams
SELECT 
    m.match_id,
    m.home_team_name,
    m.away_team_name
FROM {{ var('catalog') }}.silver.stg_matches m
LEFT JOIN {{ var('catalog') }}.silver.stg_teams t1 
    ON m.home_team_id = t1.team_id AND t1.is_current = true
LEFT JOIN {{ var('catalog') }}.silver.stg_teams t2 
    ON m.away_team_id = t2.team_id AND t2.is_current = true
WHERE m.is_current = true
  AND (t1.team_id IS NULL OR t2.team_id IS NULL)
"""

with open(f"{dbt_project_path}/tests/test_referential_integrity.sql", "w") as f:
    f.write(custom_tests)

# Test: Standings completeness
standings_test = """
-- Test: Elke competitie heeft 18-20 teams
WITH team_counts AS (
    SELECT 
        competition_name,
        COUNT(DISTINCT team_id) as team_count
    FROM {{ var('catalog') }}.silver.stg_standings
    WHERE is_current = true
    GROUP BY competition_name
)
SELECT *
FROM team_counts
WHERE team_count < 18 OR team_count > 20
"""

with open(f"{dbt_project_path}/tests/test_standings_completeness.sql", "w") as f:
    f.write(standings_test)

# Test: Form string validity
form_test = """
-- Test: Form string bevat alleen W, D, L
SELECT team, form_string
FROM {{ var('catalog') }}.gold.agg_team_form
WHERE NOT REGEXP_LIKE(form_string, '^[WDL]+$')
"""

with open(f"{dbt_project_path}/tests/test_form_string_valid.sql", "w") as f:
    f.write(form_test)

print("✅ Custom tests aangemaakt")

# COMMAND ----------

# packages.yml voor dbt_utils
packages_yml = """
packages:
  - package: dbt-labs/dbt_utils
    version: "1.3.0"
"""

with open(f"{dbt_project_path}/packages.yml", "w") as f:
    f.write(packages_yml)

print("✅ packages.yml aangemaakt")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Voer Data Quality Tests uit via SQL

# COMMAND ----------

# We voeren de tests direct uit via Spark SQL (zonder dbt cli)
# Dit werkt beter in Databricks notebooks

from datetime import datetime
from pyspark.sql import functions as F

CATALOG = "football_data"
test_results = []

def run_test(test_name: str, query: str, should_be_empty: bool = True):
    """Voer een test uit en rapporteer resultaat."""
    try:
        df = spark.sql(query)
        count = df.count()
        
        if should_be_empty:
            passed = count == 0
            message = f"{count} failures" if count > 0 else "OK"
        else:
            passed = count > 0
            message = "OK" if count > 0 else "No data found"
        
        test_results.append({
            "test_name": test_name,
            "status": "✅ PASS" if passed else "❌ FAIL",
            "details": message,
            "timestamp": datetime.now().isoformat()
        })
        
        if not passed and count > 0:
            print(f"\n⚠️ Failures for {test_name}:")
            df.show(5, truncate=False)
            
    except Exception as e:
        test_results.append({
            "test_name": test_name,
            "status": "⚠️ ERROR",
            "details": str(e)[:100],
            "timestamp": datetime.now().isoformat()
        })

print("Test framework ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Tests

# COMMAND ----------

print("=" * 60)
print("🔍 SILVER LAYER TESTS")
print("=" * 60)

# Test: stg_matches - not null match_id
run_test(
    "silver.stg_matches.match_id_not_null",
    f"SELECT * FROM {CATALOG}.silver.stg_matches WHERE match_id IS NULL"
)

# Test: stg_matches - valid competition codes
run_test(
    "silver.stg_matches.valid_competition_code",
    f"""
    SELECT DISTINCT competition_code 
    FROM {CATALOG}.silver.stg_matches 
    WHERE competition_code NOT IN ('DED', 'PL', 'PD', 'SA')
    """
)

# Test: stg_matches - unique match_id for current records
run_test(
    "silver.stg_matches.unique_current_match_id",
    f"""
    SELECT match_id, COUNT(*) as cnt
    FROM {CATALOG}.silver.stg_matches 
    WHERE is_current = true
    GROUP BY match_id
    HAVING COUNT(*) > 1
    """
)

# Test: stg_matches - valid goals range
run_test(
    "silver.stg_matches.valid_goals_range",
    f"""
    SELECT match_id, home_goals, away_goals
    FROM {CATALOG}.silver.stg_matches 
    WHERE is_finished = true 
      AND (home_goals < 0 OR home_goals > 20 OR away_goals < 0 OR away_goals > 20)
    """
)

# Test: stg_teams - not null
run_test(
    "silver.stg_teams.team_id_not_null",
    f"SELECT * FROM {CATALOG}.silver.stg_teams WHERE team_id IS NULL"
)

run_test(
    "silver.stg_teams.team_name_not_null",
    f"SELECT * FROM {CATALOG}.silver.stg_teams WHERE team_name IS NULL"
)

# Test: stg_standings - valid positions
run_test(
    "silver.stg_standings.valid_position_range",
    f"""
    SELECT team_name, position
    FROM {CATALOG}.silver.stg_standings 
    WHERE is_current = true AND (position < 1 OR position > 20)
    """
)

# Test: stg_standings - valid points
run_test(
    "silver.stg_standings.valid_points_range",
    f"""
    SELECT team_name, points
    FROM {CATALOG}.silver.stg_standings 
    WHERE is_current = true AND (points < 0 OR points > 150)
    """
)

print("\nSilver tests completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Tests

# COMMAND ----------

print("=" * 60)
print("🔍 GOLD LAYER TESTS")
print("=" * 60)

# Test: fct_match_results - unique match_id
run_test(
    "gold.fct_match_results.unique_match_id",
    f"""
    SELECT match_id, COUNT(*) as cnt
    FROM {CATALOG}.gold.fct_match_results 
    GROUP BY match_id
    HAVING COUNT(*) > 1
    """
)

# Test: fct_match_results - valid points
run_test(
    "gold.fct_match_results.valid_home_points",
    f"""
    SELECT match_id, home_points
    FROM {CATALOG}.gold.fct_match_results 
    WHERE home_points NOT IN (0, 1, 3)
    """
)

run_test(
    "gold.fct_match_results.valid_away_points",
    f"""
    SELECT match_id, away_points
    FROM {CATALOG}.gold.fct_match_results 
    WHERE away_points NOT IN (0, 1, 3)
    """
)

# Test: fct_match_results - points consistency
run_test(
    "gold.fct_match_results.points_add_up",
    f"""
    SELECT match_id, home_points, away_points
    FROM {CATALOG}.gold.fct_match_results 
    WHERE home_points + away_points NOT IN (2, 3)
    """
)

# Test: dim_teams - unique team_id
run_test(
    "gold.dim_teams.unique_team_id",
    f"""
    SELECT team_id, COUNT(*) as cnt
    FROM {CATALOG}.gold.dim_teams 
    GROUP BY team_id
    HAVING COUNT(*) > 1
    """
)

# Test: dim_teams - valid position
run_test(
    "gold.dim_teams.valid_position",
    f"""
    SELECT team_name, current_position
    FROM {CATALOG}.gold.dim_teams 
    WHERE current_position IS NOT NULL 
      AND (current_position < 1 OR current_position > 20)
    """
)

# Test: agg_team_form - valid form points
run_test(
    "gold.agg_team_form.valid_form_points",
    f"""
    SELECT team, form_points
    FROM {CATALOG}.gold.agg_team_form 
    WHERE form_points < 0 OR form_points > 15
    """
)

# Test: agg_team_form - valid form string
run_test(
    "gold.agg_team_form.valid_form_string",
    f"""
    SELECT team, form_string
    FROM {CATALOG}.gold.agg_team_form 
    WHERE NOT regexp_like(form_string, '^[WDL]+$')
    """
)

# Test: agg_head_to_head - positive matches
run_test(
    "gold.agg_head_to_head.positive_matches",
    f"""
    SELECT team, opponent, matches_played
    FROM {CATALOG}.gold.agg_head_to_head 
    WHERE matches_played < 1
    """
)

print("\nGold tests completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Referential Integrity Tests

# COMMAND ----------

print("=" * 60)
print("🔍 REFERENTIAL INTEGRITY TESTS")
print("=" * 60)

# Test: Matches reference valid teams
run_test(
    "ref_integrity.matches_have_valid_home_team",
    f"""
    SELECT m.match_id, m.home_team_name
    FROM {CATALOG}.silver.stg_matches m
    LEFT JOIN {CATALOG}.silver.stg_teams t 
        ON m.home_team_id = t.team_id AND t.is_current = true
    WHERE m.is_current = true AND t.team_id IS NULL
    """
)

run_test(
    "ref_integrity.matches_have_valid_away_team",
    f"""
    SELECT m.match_id, m.away_team_name
    FROM {CATALOG}.silver.stg_matches m
    LEFT JOIN {CATALOG}.silver.stg_teams t 
        ON m.away_team_id = t.team_id AND t.is_current = true
    WHERE m.is_current = true AND t.team_id IS NULL
    """
)

# Test: All competitions have standings
run_test(
    "ref_integrity.all_competitions_have_standings",
    f"""
    SELECT DISTINCT m.competition_code
    FROM {CATALOG}.silver.stg_matches m
    LEFT JOIN {CATALOG}.silver.stg_standings s 
        ON m.competition_code = s.competition_code AND s.is_current = true
    WHERE m.is_current = true AND s.competition_code IS NULL
    """
)

print("\nReferential integrity tests completed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Freshness Tests

# COMMAND ----------

print("=" * 60)
print("🔍 DATA FRESHNESS TESTS")
print("=" * 60)

# Check data freshness
freshness_query = f"""
SELECT 
    'silver.stg_matches' as table_name,
    MAX(transformed_at) as last_updated,
    DATEDIFF(CURRENT_TIMESTAMP(), MAX(transformed_at)) as days_old
FROM {CATALOG}.silver.stg_matches
UNION ALL
SELECT 
    'silver.stg_standings',
    MAX(transformed_at),
    DATEDIFF(CURRENT_TIMESTAMP(), MAX(transformed_at))
FROM {CATALOG}.silver.stg_standings
UNION ALL
SELECT 
    'gold.fct_match_results',
    MAX(transformed_at),
    DATEDIFF(CURRENT_TIMESTAMP(), MAX(transformed_at))
FROM {CATALOG}.gold.fct_match_results
"""

df_freshness = spark.sql(freshness_query)
display(df_freshness)

# Test: Data should not be older than 7 days
run_test(
    "freshness.data_not_stale",
    f"""
    SELECT 'silver.stg_matches' as tbl, MAX(transformed_at) as last_updated
    FROM {CATALOG}.silver.stg_matches
    HAVING DATEDIFF(CURRENT_TIMESTAMP(), MAX(transformed_at)) > 7
    UNION ALL
    SELECT 'silver.stg_standings', MAX(transformed_at)
    FROM {CATALOG}.silver.stg_standings
    HAVING DATEDIFF(CURRENT_TIMESTAMP(), MAX(transformed_at)) > 7
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test Resultaten Rapport

# COMMAND ----------

# Maak rapport DataFrame
df_results = spark.createDataFrame(test_results)

# Samenvatting
total_tests = len(test_results)
passed = len([t for t in test_results if "PASS" in t["status"]])
failed = len([t for t in test_results if "FAIL" in t["status"]])
errors = len([t for t in test_results if "ERROR" in t["status"]])

print("=" * 60)
print("📊 DATA QUALITY RAPPORT")
print("=" * 60)
print(f"""
Totaal tests:    {total_tests}
✅ Passed:       {passed}
❌ Failed:       {failed}
⚠️ Errors:       {errors}

Success Rate:    {round(100 * passed / total_tests, 1)}%
""")
print("=" * 60)

# COMMAND ----------

# Toon alle resultaten
display(df_results.orderBy("status", "test_name"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Sla Rapport op

# COMMAND ----------

# Sla resultaten op in een tabel voor historische tracking
report_table = f"{CATALOG}.gold.dq_test_results"

df_results_with_run = df_results.withColumn("run_id", F.lit(datetime.now().strftime("%Y%m%d_%H%M%S")))

df_results_with_run.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(report_table)

print(f"✅ Resultaten opgeslagen in {report_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Historische Trend

# COMMAND ----------

# Toon historische test resultaten
display(spark.sql(f"""
SELECT 
    run_id,
    COUNT(*) as total_tests,
    SUM(CASE WHEN status LIKE '%PASS%' THEN 1 ELSE 0 END) as passed,
    SUM(CASE WHEN status LIKE '%FAIL%' THEN 1 ELSE 0 END) as failed,
    ROUND(100.0 * SUM(CASE WHEN status LIKE '%PASS%' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate
FROM {report_table}
GROUP BY run_id
ORDER BY run_id DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Failures Detail

# COMMAND ----------

# Toon details van gefaalde tests
if failed > 0:
    print("❌ GEFAALDE TESTS:")
    for t in test_results:
        if "FAIL" in t["status"]:
            print(f"\n  • {t['test_name']}")
            print(f"    Details: {t['details']}")
else:
    print("✅ Alle tests geslaagd!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Samenvatting
# MAGIC 
# MAGIC Dit notebook voert de volgende data quality checks uit:
# MAGIC 
# MAGIC **Silver Layer:**
# MAGIC - Not null checks op key columns
# MAGIC - Valid value ranges (goals, positions, points)
# MAGIC - Unique constraints op current records
# MAGIC - Valid competition codes
# MAGIC 
# MAGIC **Gold Layer:**
# MAGIC - Unique constraints op fact/dim keys
# MAGIC - Valid points values (0, 1, 3)
# MAGIC - Points consistency (moet optellen tot 2 of 3)
# MAGIC - Form string validation
# MAGIC 
# MAGIC **Referential Integrity:**
# MAGIC - Matches reference valid teams
# MAGIC - All competitions have standings
# MAGIC 
# MAGIC **Freshness:**
# MAGIC - Data niet ouder dan 7 dagen
