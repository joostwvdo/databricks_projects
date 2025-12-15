# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Player Statistics from FBref
# MAGIC
# MAGIC Dit notebook haalt gedetailleerde speler statistieken op van FBref.
# MAGIC
# MAGIC ## Statistiek Types
# MAGIC - **Standard**: Goals, assists, minutes, starts
# MAGIC - **Shooting**: Shots, xG, shot accuracy
# MAGIC - **Passing**: Pass completion, key passes, xA
# MAGIC - **Defense**: Tackles, interceptions, blocks
# MAGIC - **GCA**: Goal & Shot Creating Actions
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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuratie

# COMMAND ----------

CATALOG = "football_data"
SCHEMA = "bronze"
TABLE_PLAYER_STATS = "player_stats"

COMPETITIONS = {
    "Eredivisie": {"id": "23", "country": "NED"},
    "Premier League": {"id": "9", "country": "ENG"},
    "La Liga": {"id": "12", "country": "ESP"},
    "Serie A": {"id": "11", "country": "ITA"},
}

SEASON = "2024-2025"
BASE_URL = "https://fbref.com/en/comps"
REQUEST_DELAY = 5  # Be respectful to FBref

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

player_stats_schema = StructType([
    StructField("source", StringType(), True),
    StructField("stat_type", StringType(), True),
    StructField("competition_name", StringType(), True),
    StructField("season", StringType(), True),
    StructField("player_name", StringType(), True),
    StructField("nationality", StringType(), True),
    StructField("position", StringType(), True),
    StructField("team_name", StringType(), True),
    StructField("age", StringType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("matches_played", IntegerType(), True),
    StructField("starts", IntegerType(), True),
    StructField("minutes", IntegerType(), True),
    StructField("nineties", DoubleType(), True),
    # Goals & Assists
    StructField("goals", IntegerType(), True),
    StructField("assists", IntegerType(), True),
    StructField("goals_assists", IntegerType(), True),
    StructField("non_penalty_goals", IntegerType(), True),
    StructField("penalty_goals", IntegerType(), True),
    StructField("penalty_attempts", IntegerType(), True),
    # Cards
    StructField("yellow_cards", IntegerType(), True),
    StructField("red_cards", IntegerType(), True),
    # xG Stats
    StructField("xg", DoubleType(), True),
    StructField("npxg", DoubleType(), True),
    StructField("xa", DoubleType(), True),
    StructField("xg_xa", DoubleType(), True),
    # Per 90 stats
    StructField("goals_per90", DoubleType(), True),
    StructField("assists_per90", DoubleType(), True),
    StructField("xg_per90", DoubleType(), True),
    StructField("xa_per90", DoubleType(), True),
    # Shooting
    StructField("shots", IntegerType(), True),
    StructField("shots_on_target", IntegerType(), True),
    StructField("shot_accuracy", DoubleType(), True),
    StructField("shots_per90", DoubleType(), True),
    # Passing
    StructField("passes_completed", IntegerType(), True),
    StructField("passes_attempted", IntegerType(), True),
    StructField("pass_completion_pct", DoubleType(), True),
    StructField("key_passes", IntegerType(), True),
    StructField("passes_into_final_third", IntegerType(), True),
    StructField("passes_into_penalty_area", IntegerType(), True),
    StructField("progressive_passes", IntegerType(), True),
    # Defense
    StructField("tackles", IntegerType(), True),
    StructField("tackles_won", IntegerType(), True),
    StructField("interceptions", IntegerType(), True),
    StructField("blocks", IntegerType(), True),
    StructField("clearances", IntegerType(), True),
    # GCA
    StructField("sca", IntegerType(), True),
    StructField("sca_per90", DoubleType(), True),
    StructField("gca", IntegerType(), True),
    StructField("gca_per90", DoubleType(), True),
    # Metadata
    StructField("ingest_timestamp", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Functions

# COMMAND ----------

def get_player_stats_url(comp_id: str, season: str, competition: str, stat_type: str) -> str:
    """Generate URL for player stats."""
    comp_name = competition.replace(" ", "-")
    if stat_type == "standard":
        return f"{BASE_URL}/{comp_id}/{season}/stats/players/{season}-{comp_name}-Stats"
    elif stat_type == "shooting":
        return f"{BASE_URL}/{comp_id}/{season}/shooting/players/{season}-{comp_name}-Stats"
    elif stat_type == "passing":
        return f"{BASE_URL}/{comp_id}/{season}/passing/players/{season}-{comp_name}-Stats"
    elif stat_type == "defense":
        return f"{BASE_URL}/{comp_id}/{season}/defense/players/{season}-{comp_name}-Stats"
    elif stat_type == "gca":
        return f"{BASE_URL}/{comp_id}/{season}/gca/players/{season}-{comp_name}-Stats"
    return ""


def fetch_player_stats(competition: str, comp_id: str, season: str, stat_type: str) -> pd.DataFrame:
    """Fetch player statistics from FBref."""
    url = get_player_stats_url(comp_id, season, competition, stat_type)
    print(f"  Fetching {stat_type}: {url}")

    time.sleep(REQUEST_DELAY)

    try:
        tables = pd.read_html(url)
        for table in tables:
            cols = [str(c) for c in table.columns.tolist()]
            if any("Player" in str(c) for c in cols):
                table["competition"] = competition
                table["season"] = season
                table["stat_type"] = stat_type
                return table
    except Exception as e:
        print(f"    Error: {e}")

    return pd.DataFrame()


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten multi-level column names."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(str(c) for c in col).strip() for col in df.columns.values]
    return df


def safe_int(val):
    """Safely convert to int."""
    try:
        if pd.isna(val):
            return None
        return int(float(val))
    except:
        return None


def safe_float(val):
    """Safely convert to float."""
    try:
        if pd.isna(val):
            return None
        return float(val)
    except:
        return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Functions

# COMMAND ----------

def transform_standard_stats(df: pd.DataFrame, competition: str) -> list[dict]:
    """Transform standard player stats."""
    if df.empty:
        return []

    df = flatten_columns(df.copy())
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    for _, row in df.iterrows():
        try:
            # Find player name column
            player_col = next((c for c in df.columns if 'Player' in c and 'player' not in c.lower()), None)
            player_name = str(row.get(player_col, "")) if player_col else ""

            if not player_name or player_name == "nan" or player_name.strip() == "":
                continue

            player_row = {
                "source": "fbref",
                "stat_type": "standard",
                "competition_name": competition,
                "season": SEASON,
                "player_name": player_name,
                "nationality": str(row.get(next((c for c in df.columns if 'Nation' in c), ""), "")),
                "position": str(row.get(next((c for c in df.columns if 'Pos' in c), ""), "")),
                "team_name": str(row.get(next((c for c in df.columns if 'Squad' in c), ""), "")),
                "age": str(row.get(next((c for c in df.columns if 'Age' in c), ""), "")),
                "birth_year": safe_int(row.get(next((c for c in df.columns if 'Born' in c), ""), None)),

                # Playing time
                "matches_played": safe_int(row.get(next((c for c in df.columns if c.endswith('_MP') or c == 'MP'), ""), None)),
                "starts": safe_int(row.get(next((c for c in df.columns if c.endswith('_Starts') or c == 'Starts'), ""), None)),
                "minutes": safe_int(row.get(next((c for c in df.columns if c.endswith('_Min') or c == 'Min'), ""), None)),
                "nineties": safe_float(row.get(next((c for c in df.columns if '90s' in c), ""), None)),

                # Goals & Assists
                "goals": safe_int(row.get(next((c for c in df.columns if c.endswith('_Gls') or c == 'Gls'), ""), None)),
                "assists": safe_int(row.get(next((c for c in df.columns if c.endswith('_Ast') or c == 'Ast'), ""), None)),
                "goals_assists": safe_int(row.get(next((c for c in df.columns if 'G+A' in c or 'G-A' in c), ""), None)),
                "non_penalty_goals": safe_int(row.get(next((c for c in df.columns if 'G-PK' in c or 'npG' in c.lower()), ""), None)),
                "penalty_goals": safe_int(row.get(next((c for c in df.columns if c.endswith('_PK') or c == 'PK'), ""), None)),
                "penalty_attempts": safe_int(row.get(next((c for c in df.columns if 'PKatt' in c), ""), None)),

                # Cards
                "yellow_cards": safe_int(row.get(next((c for c in df.columns if c.endswith('_CrdY') or c == 'CrdY'), ""), None)),
                "red_cards": safe_int(row.get(next((c for c in df.columns if c.endswith('_CrdR') or c == 'CrdR'), ""), None)),

                # xG Stats
                "xg": safe_float(row.get(next((c for c in df.columns if c.endswith('_xG') or c == 'xG'), ""), None)),
                "npxg": safe_float(row.get(next((c for c in df.columns if 'npxG' in c), ""), None)),
                "xa": safe_float(row.get(next((c for c in df.columns if c.endswith('_xAG') or c == 'xAG' or c == 'xA'), ""), None)),
                "xg_xa": safe_float(row.get(next((c for c in df.columns if 'xG+xAG' in c), ""), None)),

                # Per 90
                "goals_per90": safe_float(row.get(next((c for c in df.columns if 'Gls' in c and '90' in c), ""), None)),
                "assists_per90": safe_float(row.get(next((c for c in df.columns if 'Ast' in c and '90' in c), ""), None)),
                "xg_per90": safe_float(row.get(next((c for c in df.columns if 'xG' in c and '90' in c and 'xAG' not in c), ""), None)),
                "xa_per90": safe_float(row.get(next((c for c in df.columns if 'xAG' in c and '90' in c), ""), None)),

                # Placeholder for other stat types
                "shots": None,
                "shots_on_target": None,
                "shot_accuracy": None,
                "shots_per90": None,
                "passes_completed": None,
                "passes_attempted": None,
                "pass_completion_pct": None,
                "key_passes": None,
                "passes_into_final_third": None,
                "passes_into_penalty_area": None,
                "progressive_passes": None,
                "tackles": None,
                "tackles_won": None,
                "interceptions": None,
                "blocks": None,
                "clearances": None,
                "sca": None,
                "sca_per90": None,
                "gca": None,
                "gca_per90": None,

                "ingest_timestamp": ingest_timestamp,
            }
            rows.append(player_row)

        except Exception as e:
            continue

    return rows


def enrich_with_shooting(rows: list[dict], shooting_df: pd.DataFrame) -> list[dict]:
    """Enrich player rows with shooting stats."""
    if shooting_df.empty:
        return rows

    shooting_df = flatten_columns(shooting_df.copy())

    # Create lookup by player + team
    shooting_lookup = {}
    player_col = next((c for c in shooting_df.columns if 'Player' in c and 'player' not in c.lower()), None)
    squad_col = next((c for c in shooting_df.columns if 'Squad' in c), None)

    for _, row in shooting_df.iterrows():
        player = str(row.get(player_col, ""))
        team = str(row.get(squad_col, ""))
        key = f"{player}_{team}"
        shooting_lookup[key] = row

    for player_row in rows:
        key = f"{player_row['player_name']}_{player_row['team_name']}"
        if key in shooting_lookup:
            srow = shooting_lookup[key]
            player_row["shots"] = safe_int(srow.get(next((c for c in shooting_df.columns if c.endswith('_Sh') or c == 'Sh'), ""), None))
            player_row["shots_on_target"] = safe_int(srow.get(next((c for c in shooting_df.columns if 'SoT' in c and '/' not in c and '%' not in c), ""), None))
            player_row["shot_accuracy"] = safe_float(srow.get(next((c for c in shooting_df.columns if 'SoT%' in c), ""), None))
            player_row["shots_per90"] = safe_float(srow.get(next((c for c in shooting_df.columns if 'Sh/90' in c or ('Sh' in c and '90' in c)), ""), None))

    return rows


def enrich_with_passing(rows: list[dict], passing_df: pd.DataFrame) -> list[dict]:
    """Enrich player rows with passing stats."""
    if passing_df.empty:
        return rows

    passing_df = flatten_columns(passing_df.copy())

    passing_lookup = {}
    player_col = next((c for c in passing_df.columns if 'Player' in c and 'player' not in c.lower()), None)
    squad_col = next((c for c in passing_df.columns if 'Squad' in c), None)

    for _, row in passing_df.iterrows():
        player = str(row.get(player_col, ""))
        team = str(row.get(squad_col, ""))
        key = f"{player}_{team}"
        passing_lookup[key] = row

    for player_row in rows:
        key = f"{player_row['player_name']}_{player_row['team_name']}"
        if key in passing_lookup:
            prow = passing_lookup[key]
            player_row["passes_completed"] = safe_int(prow.get(next((c for c in passing_df.columns if 'Cmp' in c and 'Total' in c), ""), None))
            player_row["passes_attempted"] = safe_int(prow.get(next((c for c in passing_df.columns if 'Att' in c and 'Total' in c), ""), None))
            player_row["pass_completion_pct"] = safe_float(prow.get(next((c for c in passing_df.columns if 'Cmp%' in c and 'Total' in c), ""), None))
            player_row["key_passes"] = safe_int(prow.get(next((c for c in passing_df.columns if 'KP' in c), ""), None))
            player_row["passes_into_final_third"] = safe_int(prow.get(next((c for c in passing_df.columns if '1/3' in c), ""), None))
            player_row["passes_into_penalty_area"] = safe_int(prow.get(next((c for c in passing_df.columns if 'PPA' in c), ""), None))
            player_row["progressive_passes"] = safe_int(prow.get(next((c for c in passing_df.columns if 'PrgP' in c), ""), None))

    return rows


def enrich_with_defense(rows: list[dict], defense_df: pd.DataFrame) -> list[dict]:
    """Enrich player rows with defensive stats."""
    if defense_df.empty:
        return rows

    defense_df = flatten_columns(defense_df.copy())

    defense_lookup = {}
    player_col = next((c for c in defense_df.columns if 'Player' in c and 'player' not in c.lower()), None)
    squad_col = next((c for c in defense_df.columns if 'Squad' in c), None)

    for _, row in defense_df.iterrows():
        player = str(row.get(player_col, ""))
        team = str(row.get(squad_col, ""))
        key = f"{player}_{team}"
        defense_lookup[key] = row

    for player_row in rows:
        key = f"{player_row['player_name']}_{player_row['team_name']}"
        if key in defense_lookup:
            drow = defense_lookup[key]
            player_row["tackles"] = safe_int(drow.get(next((c for c in defense_df.columns if c.endswith('_Tkl') or c == 'Tkl'), ""), None))
            player_row["tackles_won"] = safe_int(drow.get(next((c for c in defense_df.columns if 'TklW' in c), ""), None))
            player_row["interceptions"] = safe_int(drow.get(next((c for c in defense_df.columns if c.endswith('_Int') or c == 'Int'), ""), None))
            player_row["blocks"] = safe_int(drow.get(next((c for c in defense_df.columns if c.endswith('_Blocks') or c == 'Blocks'), ""), None))
            player_row["clearances"] = safe_int(drow.get(next((c for c in defense_df.columns if c.endswith('_Clr') or c == 'Clr'), ""), None))

    return rows


def enrich_with_gca(rows: list[dict], gca_df: pd.DataFrame) -> list[dict]:
    """Enrich player rows with GCA stats."""
    if gca_df.empty:
        return rows

    gca_df = flatten_columns(gca_df.copy())

    gca_lookup = {}
    player_col = next((c for c in gca_df.columns if 'Player' in c and 'player' not in c.lower()), None)
    squad_col = next((c for c in gca_df.columns if 'Squad' in c), None)

    for _, row in gca_df.iterrows():
        player = str(row.get(player_col, ""))
        team = str(row.get(squad_col, ""))
        key = f"{player}_{team}"
        gca_lookup[key] = row

    for player_row in rows:
        key = f"{player_row['player_name']}_{player_row['team_name']}"
        if key in gca_lookup:
            grow = gca_lookup[key]
            player_row["sca"] = safe_int(grow.get(next((c for c in gca_df.columns if c.endswith('_SCA') or c == 'SCA'), ""), None))
            player_row["sca_per90"] = safe_float(grow.get(next((c for c in gca_df.columns if 'SCA90' in c), ""), None))
            player_row["gca"] = safe_int(grow.get(next((c for c in gca_df.columns if c.endswith('_GCA') or c == 'GCA'), ""), None))
            player_row["gca_per90"] = safe_float(grow.get(next((c for c in gca_df.columns if 'GCA90' in c), ""), None))

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

all_player_stats = []

for comp_name, comp_info in COMPETITIONS.items():
    print(f"\n{'='*60}")
    print(f"🏆 Fetching player stats for {comp_name}...")

    try:
        # 1. Standard stats (base)
        standard_df = fetch_player_stats(comp_name, comp_info["id"], SEASON, "standard")
        if standard_df.empty:
            print(f"  ⚠️ No standard stats found")
            continue

        rows = transform_standard_stats(standard_df, comp_name)
        print(f"  ✅ Standard: {len(rows)} players")

        # 2. Shooting stats
        shooting_df = fetch_player_stats(comp_name, comp_info["id"], SEASON, "shooting")
        if not shooting_df.empty:
            rows = enrich_with_shooting(rows, shooting_df)
            print(f"  ✅ Shooting stats added")

        # 3. Passing stats
        passing_df = fetch_player_stats(comp_name, comp_info["id"], SEASON, "passing")
        if not passing_df.empty:
            rows = enrich_with_passing(rows, passing_df)
            print(f"  ✅ Passing stats added")

        # 4. Defensive stats
        defense_df = fetch_player_stats(comp_name, comp_info["id"], SEASON, "defense")
        if not defense_df.empty:
            rows = enrich_with_defense(rows, defense_df)
            print(f"  ✅ Defensive stats added")

        # 5. GCA stats
        gca_df = fetch_player_stats(comp_name, comp_info["id"], SEASON, "gca")
        if not gca_df.empty:
            rows = enrich_with_gca(rows, gca_df)
            print(f"  ✅ GCA stats added")

        all_player_stats.extend(rows)

    except Exception as e:
        print(f"  ❌ Error: {e}")

print(f"\n{'='*60}")
print(f"📊 Total: {len(all_player_stats)} player records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opslaan naar Delta Table

# COMMAND ----------

if all_player_stats:
    df_players = spark.createDataFrame(all_player_stats, schema=player_stats_schema)

    (
        df_players.write.format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.{TABLE_PLAYER_STATS}")
    )

    print(f"✅ Saved {df_players.count()} player stats to {CATALOG}.{SCHEMA}.{TABLE_PLAYER_STATS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificatie

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT competition_name, COUNT(DISTINCT player_name) as player_count
# MAGIC FROM football_data.bronze.player_stats
# MAGIC GROUP BY competition_name
# MAGIC ORDER BY player_count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top scorers
# MAGIC SELECT
# MAGIC   player_name,
# MAGIC   team_name,
# MAGIC   competition_name,
# MAGIC   goals,
# MAGIC   assists,
# MAGIC   xg,
# MAGIC   npxg,
# MAGIC   minutes
# MAGIC FROM football_data.bronze.player_stats
# MAGIC WHERE goals IS NOT NULL
# MAGIC ORDER BY goals DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Best xG performers (outperforming expected goals)
# MAGIC SELECT
# MAGIC   player_name,
# MAGIC   team_name,
# MAGIC   competition_name,
# MAGIC   goals,
# MAGIC   ROUND(xg, 2) as xg,
# MAGIC   ROUND(goals - xg, 2) as goals_vs_xg,
# MAGIC   minutes
# MAGIC FROM football_data.bronze.player_stats
# MAGIC WHERE xg IS NOT NULL AND minutes >= 450
# MAGIC ORDER BY (goals - xg) DESC
# MAGIC LIMIT 20
