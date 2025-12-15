# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Matches
# MAGIC Haalt wedstrijddata op van Football-Data.org API en slaat op in Bronze layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuratie

# COMMAND ----------

# Configuratie
CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"

# Competities om op te halen
COMPETITIONS = {
    "DED": "Eredivisie",
    "PL": "Premier League", 
    "PD": "La Liga",
    "SA": "Serie A"
}

API_BASE_URL = "https://api.football-data.org/v4"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. API Key ophalen uit Secrets

# COMMAND ----------

# Haal API key uit Databricks secrets
API_KEY = dbutils.secrets.get(scope="football-data", key="api-key")
print("✅ API key opgehaald uit secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper functies

# COMMAND ----------

import requests
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, BooleanType

def get_api_headers():
    return {"X-Auth-Token": API_KEY}

def fetch_matches(competition_code: str) -> list:
    """Haal wedstrijden op voor een competitie."""
    url = f"{API_BASE_URL}/competitions/{competition_code}/matches"
    response = requests.get(url, headers=get_api_headers())
    
    if response.status_code == 200:
        return response.json().get("matches", [])
    else:
        print(f"❌ Error fetching {competition_code}: {response.status_code}")
        return []

def fetch_standings(competition_code: str) -> list:
    """Haal stand op voor een competitie."""
    url = f"{API_BASE_URL}/competitions/{competition_code}/standings"
    response = requests.get(url, headers=get_api_headers())
    
    if response.status_code == 200:
        standings = response.json().get("standings", [])
        if standings:
            return standings[0].get("table", [])
    return []

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data ophalen en transformeren

# COMMAND ----------

from pyspark.sql import Row

all_matches = []
ingest_timestamp = datetime.utcnow().isoformat()

for code, name in COMPETITIONS.items():
    print(f"🔄 Ophalen {name} ({code})...")
    
    matches = fetch_matches(code)
    print(f"   Gevonden: {len(matches)} wedstrijden")
    
    for match in matches:
        score = match.get("score", {})
        full_time = score.get("fullTime", {})
        
        all_matches.append(Row(
            match_id=match.get("id"),
            competition_code=code,
            competition_name=name,
            season=match.get("season", {}).get("id"),
            matchday=match.get("matchday"),
            match_date=match.get("utcDate"),
            status=match.get("status"),
            home_team_id=match.get("homeTeam", {}).get("id"),
            home_team_name=match.get("homeTeam", {}).get("name"),
            away_team_id=match.get("awayTeam", {}).get("id"),
            away_team_name=match.get("awayTeam", {}).get("name"),
            home_score=full_time.get("home"),
            away_score=full_time.get("away"),
            winner=score.get("winner"),
            ingest_timestamp=ingest_timestamp
        ))

print(f"\n✅ Totaal: {len(all_matches)} wedstrijden opgehaald")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Opslaan naar Bronze Layer

# COMMAND ----------

# Maak DataFrame
df_matches = spark.createDataFrame(all_matches)

# Gebruik catalog
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# Schrijf naar Delta table
table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.matches"
df_matches.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"✅ Data opgeslagen in {table_name}")
print(f"   Aantal records: {df_matches.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Standings ophalen

# COMMAND ----------

all_standings = []

for code, name in COMPETITIONS.items():
    print(f"🔄 Ophalen stand {name}...")
    
    standings = fetch_standings(code)
    
    for team in standings:
        all_standings.append(Row(
            competition_code=code,
            competition_name=name,
            position=team.get("position"),
            team_id=team.get("team", {}).get("id"),
            team_name=team.get("team", {}).get("name"),
            played_games=team.get("playedGames"),
            won=team.get("won"),
            draw=team.get("draw"),
            lost=team.get("lost"),
            points=team.get("points"),
            goals_for=team.get("goalsFor"),
            goals_against=team.get("goalsAgainst"),
            goal_difference=team.get("goalDifference"),
            ingest_timestamp=ingest_timestamp
        ))

print(f"\n✅ Totaal: {len(all_standings)} team standings opgehaald")

# COMMAND ----------

# Opslaan standings
df_standings = spark.createDataFrame(all_standings)
standings_table = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.standings"
df_standings.write.format("delta").mode("overwrite").saveAsTable(standings_table)

print(f"✅ Standings opgeslagen in {standings_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificatie

# COMMAND ----------

display(spark.sql(f"SELECT competition_name, COUNT(*) as matches FROM {table_name} GROUP BY competition_name"))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {standings_table} WHERE position <= 5 ORDER BY competition_name, position"))

# COMMAND ----------

print("🎉 Bronze layer ingestion compleet!")
