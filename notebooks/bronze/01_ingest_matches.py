# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Football Matches
# MAGIC
# MAGIC Dit notebook haalt voetbalwedstrijden op van de Football-Data.org API
# MAGIC en slaat ze op in de Bronze Delta table.

# COMMAND ----------

# MAGIC %pip install requests python-dotenv

# COMMAND ----------

import requests
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuratie

# COMMAND ----------

# Configuratie - pas aan naar je eigen settings
API_KEY = dbutils.secrets.get(scope="football-data", key="api-key")  # type: ignore
BASE_URL = "https://api.football-data.org/v4"

CATALOG = "football_data"
SCHEMA = "bronze"
TABLE_MATCHES = "matches"
TABLE_TEAMS = "teams"

# Competities om op te halen (Eredivisie, Premier League, La Liga, Serie A)
COMPETITIONS = {
    "Eredivisie": "DED",
    "Premier League": "PL",
    "La Liga": "PD",
    "Serie A": "SA",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema definities

# COMMAND ----------

matches_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("utc_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("matchday", IntegerType(), True),
    StructField("stage", StringType(), True),
    StructField("last_updated", StringType(), True),
    StructField("competition_id", IntegerType(), True),
    StructField("competition_name", StringType(), True),
    StructField("competition_code", StringType(), True),
    StructField("home_team_id", IntegerType(), True),
    StructField("home_team_name", StringType(), True),
    StructField("home_team_short_name", StringType(), True),
    StructField("away_team_id", IntegerType(), True),
    StructField("away_team_name", StringType(), True),
    StructField("away_team_short_name", StringType(), True),
    StructField("home_score_fulltime", IntegerType(), True),
    StructField("away_score_fulltime", IntegerType(), True),
    StructField("home_score_halftime", IntegerType(), True),
    StructField("away_score_halftime", IntegerType(), True),
    StructField("winner", StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
])

teams_schema = StructType([
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("tla", StringType(), True),
    StructField("crest_url", StringType(), True),
    StructField("address", StringType(), True),
    StructField("website", StringType(), True),
    StructField("founded", IntegerType(), True),
    StructField("club_colors", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("competition_id", IntegerType(), True),
    StructField("competition_name", StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## API Functies

# COMMAND ----------

def fetch_matches(competition_code: str, season: int = None) -> list[dict]:
    """Haal wedstrijden op van de API."""
    headers = {"X-Auth-Token": API_KEY}
    params = {}
    if season:
        params["season"] = season

    response = requests.get(
        f"{BASE_URL}/competitions/{competition_code}/matches",
        headers=headers,
        params=params,
    )
    response.raise_for_status()
    return response.json()


def fetch_teams(competition_code: str) -> list[dict]:
    """Haal teams op van de API."""
    headers = {"X-Auth-Token": API_KEY}

    response = requests.get(
        f"{BASE_URL}/competitions/{competition_code}/teams",
        headers=headers,
    )
    response.raise_for_status()
    return response.json()


def transform_matches(matches_data: dict) -> list[dict]:
    """Transformeer matches naar platte structuur."""
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()
    competition = matches_data.get("competition", {})

    for match in matches_data.get("matches", []):
        row = {
            "match_id": match.get("id"),
            "utc_date": match.get("utcDate"),
            "status": match.get("status"),
            "matchday": match.get("matchday"),
            "stage": match.get("stage"),
            "last_updated": match.get("lastUpdated"),
            "competition_id": competition.get("id"),
            "competition_name": competition.get("name"),
            "competition_code": competition.get("code"),
            "home_team_id": match.get("homeTeam", {}).get("id"),
            "home_team_name": match.get("homeTeam", {}).get("name"),
            "home_team_short_name": match.get("homeTeam", {}).get("shortName"),
            "away_team_id": match.get("awayTeam", {}).get("id"),
            "away_team_name": match.get("awayTeam", {}).get("name"),
            "away_team_short_name": match.get("awayTeam", {}).get("shortName"),
            "home_score_fulltime": match.get("score", {}).get("fullTime", {}).get("home"),
            "away_score_fulltime": match.get("score", {}).get("fullTime", {}).get("away"),
            "home_score_halftime": match.get("score", {}).get("halfTime", {}).get("home"),
            "away_score_halftime": match.get("score", {}).get("halfTime", {}).get("away"),
            "winner": match.get("score", {}).get("winner"),
            "ingest_timestamp": ingest_timestamp,
        }
        rows.append(row)

    return rows


def transform_teams(teams_data: dict) -> list[dict]:
    """Transformeer teams naar platte structuur."""
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()
    competition = teams_data.get("competition", {})

    for team in teams_data.get("teams", []):
        row = {
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "short_name": team.get("shortName"),
            "tla": team.get("tla"),
            "crest_url": team.get("crest"),
            "address": team.get("address"),
            "website": team.get("website"),
            "founded": team.get("founded"),
            "club_colors": team.get("clubColors"),
            "venue": team.get("venue"),
            "competition_id": competition.get("id"),
            "competition_name": competition.get("name"),
            "ingest_timestamp": ingest_timestamp,
        }
        rows.append(row)

    return rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## Database Setup

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Ingestion

# COMMAND ----------

# Verzamel alle wedstrijden
all_matches = []
all_teams = []

for league_name, comp_code in COMPETITIONS.items():
    print(f"Fetching data for {league_name} ({comp_code})...")

    try:
        # Fetch matches
        matches_data = fetch_matches(comp_code)
        matches_rows = transform_matches(matches_data)
        all_matches.extend(matches_rows)
        print(f"  - {len(matches_rows)} matches")

        # Fetch teams
        teams_data = fetch_teams(comp_code)
        teams_rows = transform_teams(teams_data)
        all_teams.extend(teams_rows)
        print(f"  - {len(teams_rows)} teams")

    except Exception as e:
        print(f"  - Error: {e}")

print(f"\nTotal: {len(all_matches)} matches, {len(all_teams)} teams")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opslaan naar Delta Tables

# COMMAND ----------

# Matches opslaan
if all_matches:
    df_matches = spark.createDataFrame(all_matches, schema=matches_schema)

    (
        df_matches.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE_MATCHES}")
    )

    print(f"Saved {df_matches.count()} matches to {CATALOG}.{SCHEMA}.{TABLE_MATCHES}")

# COMMAND ----------

# Teams opslaan
if all_teams:
    df_teams = spark.createDataFrame(all_teams, schema=teams_schema)

    (
        df_teams.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE_TEAMS}")
    )

    print(f"Saved {df_teams.count()} teams to {CATALOG}.{SCHEMA}.{TABLE_TEAMS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificatie

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT competition_name, COUNT(*) as match_count
# MAGIC FROM football_data.bronze.matches
# MAGIC GROUP BY competition_name
# MAGIC ORDER BY match_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM football_data.bronze.matches LIMIT 10
