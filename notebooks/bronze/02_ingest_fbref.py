# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest FBref Data
# MAGIC
# MAGIC Dit notebook haalt voetbaldata op van FBref (Football Reference) via web scraping.
# MAGIC FBref biedt gedetailleerde statistieken inclusief xG (Expected Goals).
# MAGIC
# MAGIC ## Competities
# MAGIC - 🇳🇱 Eredivisie
# MAGIC - 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League
# MAGIC - 🇪🇸 La Liga
# MAGIC - 🇮🇹 Serie A

# COMMAND ----------

# MAGIC %pip install requests pandas lxml html5lib beautifulsoup4

# COMMAND ----------

import requests
import pandas as pd
import time
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuratie

# COMMAND ----------

CATALOG = "football_data"
SCHEMA = "bronze"
TABLE_FBREF_MATCHES = "fbref_matches"
TABLE_FBREF_STANDINGS = "fbref_standings"

# FBref competition IDs
COMPETITIONS = {
    "Eredivisie": {"id": "23", "country": "NED"},
    "Premier League": {"id": "9", "country": "ENG"},
    "La Liga": {"id": "12", "country": "ESP"},
    "Serie A": {"id": "11", "country": "ITA"},
}

SEASON = "2024-2025"
BASE_URL = "https://fbref.com/en/comps"
REQUEST_DELAY = 4  # FBref rate limits - be respectful

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definities

# COMMAND ----------

fbref_matches_schema = StructType([
    StructField("source", StringType(), True),
    StructField("competition_name", StringType(), True),
    StructField("season", StringType(), True),
    StructField("matchweek", StringType(), True),
    StructField("match_date", StringType(), True),
    StructField("match_time", StringType(), True),
    StructField("home_team_name", StringType(), True),
    StructField("away_team_name", StringType(), True),
    StructField("home_score", StringType(), True),
    StructField("away_score", StringType(), True),
    StructField("home_xg", DoubleType(), True),
    StructField("away_xg", DoubleType(), True),
    StructField("attendance", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("referee", StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
])

fbref_standings_schema = StructType([
    StructField("source", StringType(), True),
    StructField("competition_name", StringType(), True),
    StructField("season", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("team_name", StringType(), True),
    StructField("matches_played", IntegerType(), True),
    StructField("wins", IntegerType(), True),
    StructField("draws", IntegerType(), True),
    StructField("losses", IntegerType(), True),
    StructField("goals_for", IntegerType(), True),
    StructField("goals_against", IntegerType(), True),
    StructField("goal_difference", IntegerType(), True),
    StructField("points", IntegerType(), True),
    StructField("xg", DoubleType(), True),
    StructField("xga", DoubleType(), True),
    StructField("ingest_timestamp", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Functions

# COMMAND ----------

def get_session():
    """Create a requests session with proper headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    })
    return session


def fetch_fbref_matches(competition_name: str, comp_id: str, season: str) -> pd.DataFrame:
    """Fetch match results from FBref."""
    url = f"{BASE_URL}/{comp_id}/{season}/schedule/{season}-{competition_name.replace(' ', '-')}-Scores-and-Fixtures"

    print(f"Fetching: {url}")
    time.sleep(REQUEST_DELAY)

    try:
        tables = pd.read_html(url)
        if tables:
            df = tables[0]
            df["competition_name"] = competition_name
            df["season"] = season
            return df
    except Exception as e:
        print(f"Error fetching matches: {e}")

    return pd.DataFrame()


def fetch_fbref_standings(competition_name: str, comp_id: str, season: str) -> pd.DataFrame:
    """Fetch league standings from FBref."""
    url = f"{BASE_URL}/{comp_id}/{season}/{season}-{competition_name.replace(' ', '-')}-Stats"

    print(f"Fetching: {url}")
    time.sleep(REQUEST_DELAY)

    try:
        tables = pd.read_html(url)
        if tables:
            # First table is usually the standings
            df = tables[0]
            df["competition_name"] = competition_name
            df["season"] = season
            return df
    except Exception as e:
        print(f"Error fetching standings: {e}")

    return pd.DataFrame()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Functions

# COMMAND ----------

def transform_matches(df: pd.DataFrame, competition: str) -> list[dict]:
    """Transform FBref matches to flat rows."""
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    for _, row in df.iterrows():
        try:
            # Parse score
            score = str(row.get("Score", ""))
            home_score = None
            away_score = None
            if "–" in score:
                parts = score.split("–")
                home_score = parts[0].strip()
                away_score = parts[1].strip()

            match_row = {
                "source": "fbref",
                "competition_name": competition,
                "season": str(row.get("season", "")),
                "matchweek": str(row.get("Wk", "")),
                "match_date": str(row.get("Date", "")),
                "match_time": str(row.get("Time", "")),
                "home_team_name": str(row.get("Home", "")),
                "away_team_name": str(row.get("Away", "")),
                "home_score": home_score,
                "away_score": away_score,
                "home_xg": float(row.get("xG", 0)) if pd.notna(row.get("xG")) else None,
                "away_xg": float(row.get("xG.1", 0)) if pd.notna(row.get("xG.1")) else None,
                "attendance": str(row.get("Attendance", "")),
                "venue": str(row.get("Venue", "")),
                "referee": str(row.get("Referee", "")),
                "ingest_timestamp": ingest_timestamp,
            }
            rows.append(match_row)
        except Exception as e:
            continue

    return rows


def transform_standings(df: pd.DataFrame, competition: str) -> list[dict]:
    """Transform FBref standings to flat rows."""
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    # Handle multi-level columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(str(c) for c in col).strip() for col in df.columns.values]

    for _, row in df.iterrows():
        try:
            standing_row = {
                "source": "fbref",
                "competition_name": competition,
                "season": str(row.get("season", "")),
                "position": int(row.get("Rk", 0)) if pd.notna(row.get("Rk")) else None,
                "team_name": str(row.get("Squad", "")),
                "matches_played": int(row.get("MP", 0)) if pd.notna(row.get("MP")) else None,
                "wins": int(row.get("W", 0)) if pd.notna(row.get("W")) else None,
                "draws": int(row.get("D", 0)) if pd.notna(row.get("D")) else None,
                "losses": int(row.get("L", 0)) if pd.notna(row.get("L")) else None,
                "goals_for": int(row.get("GF", 0)) if pd.notna(row.get("GF")) else None,
                "goals_against": int(row.get("GA", 0)) if pd.notna(row.get("GA")) else None,
                "goal_difference": int(row.get("GD", 0)) if pd.notna(row.get("GD")) else None,
                "points": int(row.get("Pts", 0)) if pd.notna(row.get("Pts")) else None,
                "xg": float(row.get("xG", 0)) if pd.notna(row.get("xG")) else None,
                "xga": float(row.get("xGA", 0)) if pd.notna(row.get("xGA")) else None,
                "ingest_timestamp": ingest_timestamp,
            }
            rows.append(standing_row)
        except Exception as e:
            continue

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

all_matches = []
all_standings = []

for comp_name, comp_info in COMPETITIONS.items():
    print(f"\n{'='*50}")
    print(f"🏆 Fetching {comp_name}...")

    try:
        # Fetch matches
        matches_df = fetch_fbref_matches(comp_name, comp_info["id"], SEASON)
        if not matches_df.empty:
            matches_rows = transform_matches(matches_df, comp_name)
            all_matches.extend(matches_rows)
            print(f"  ✅ {len(matches_rows)} matches")

        # Fetch standings
        standings_df = fetch_fbref_standings(comp_name, comp_info["id"], SEASON)
        if not standings_df.empty:
            standings_rows = transform_standings(standings_df, comp_name)
            all_standings.extend(standings_rows)
            print(f"  ✅ {len(standings_rows)} teams in standings")

    except Exception as e:
        print(f"  ❌ Error: {e}")

print(f"\n{'='*50}")
print(f"📊 Total: {len(all_matches)} matches, {len(all_standings)} team standings")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opslaan naar Delta Tables

# COMMAND ----------

# Matches opslaan
if all_matches:
    df_matches = spark.createDataFrame(all_matches, schema=fbref_matches_schema)

    (
        df_matches.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE_FBREF_MATCHES}")
    )

    print(f"✅ Saved {df_matches.count()} matches to {CATALOG}.{SCHEMA}.{TABLE_FBREF_MATCHES}")

# COMMAND ----------

# Standings opslaan
if all_standings:
    df_standings = spark.createDataFrame(all_standings, schema=fbref_standings_schema)

    (
        df_standings.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE_FBREF_STANDINGS}")
    )

    print(f"✅ Saved {df_standings.count()} standings to {CATALOG}.{SCHEMA}.{TABLE_FBREF_STANDINGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificatie

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT competition_name, COUNT(*) as match_count
# MAGIC FROM football_data.bronze.fbref_matches
# MAGIC GROUP BY competition_name
# MAGIC ORDER BY match_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT competition_name, team_name, position, points, xg, xga
# MAGIC FROM football_data.bronze.fbref_standings
# MAGIC WHERE position <= 5
# MAGIC ORDER BY competition_name, position

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Matches met xG data
# MAGIC SELECT
# MAGIC   competition_name,
# MAGIC   match_date,
# MAGIC   home_team_name,
# MAGIC   away_team_name,
# MAGIC   home_score,
# MAGIC   away_score,
# MAGIC   home_xg,
# MAGIC   away_xg
# MAGIC FROM football_data.bronze.fbref_matches
# MAGIC WHERE home_xg IS NOT NULL
# MAGIC ORDER BY match_date DESC
# MAGIC LIMIT 20
