# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Ingest Teams
# MAGIC Haalt team informatie op van Football-Data.org API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuratie

# COMMAND ----------

CATALOG_NAME = "football_data"
BRONZE_SCHEMA = "bronze"

COMPETITIONS = {
    "DED": "Eredivisie",
    "PL": "Premier League", 
    "PD": "La Liga",
    "SA": "Serie A"
}

API_BASE_URL = "https://api.football-data.org/v4"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. API Key ophalen

# COMMAND ----------

API_KEY = dbutils.secrets.get(scope="football-data", key="api-key")
print("✅ API key opgehaald")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Teams ophalen

# COMMAND ----------

import requests
from datetime import datetime
from pyspark.sql import Row

def fetch_teams(competition_code: str) -> list:
    """Haal teams op voor een competitie."""
    url = f"{API_BASE_URL}/competitions/{competition_code}/teams"
    headers = {"X-Auth-Token": API_KEY}
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json().get("teams", [])
    else:
        print(f"❌ Error: {response.status_code}")
        return []

# COMMAND ----------

all_teams = []
ingest_timestamp = datetime.utcnow().isoformat()

for code, name in COMPETITIONS.items():
    print(f"🔄 Ophalen teams {name}...")
    
    teams = fetch_teams(code)
    print(f"   Gevonden: {len(teams)} teams")
    
    for team in teams:
        all_teams.append(Row(
            team_id=team.get("id"),
            team_name=team.get("name"),
            short_name=team.get("shortName"),
            tla=team.get("tla"),
            crest_url=team.get("crest"),
            address=team.get("address"),
            website=team.get("website"),
            founded=team.get("founded"),
            club_colors=team.get("clubColors"),
            venue=team.get("venue"),
            competition_code=code,
            competition_name=name,
            ingest_timestamp=ingest_timestamp
        ))

print(f"\n✅ Totaal: {len(all_teams)} teams opgehaald")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Opslaan naar Bronze Layer

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG_NAME}")

df_teams = spark.createDataFrame(all_teams)
table_name = f"{CATALOG_NAME}.{BRONZE_SCHEMA}.teams"
df_teams.write.format("delta").mode("overwrite").saveAsTable(table_name)

print(f"✅ Data opgeslagen in {table_name}")
print(f"   Aantal teams: {df_teams.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Verificatie

# COMMAND ----------

display(spark.sql(f"""
SELECT competition_name, COUNT(*) as teams 
FROM {table_name} 
GROUP BY competition_name
ORDER BY competition_name
"""))

# COMMAND ----------

display(spark.sql(f"SELECT team_name, short_name, venue, founded FROM {table_name} WHERE competition_code = 'DED' ORDER BY team_name"))

# COMMAND ----------

print("🎉 Teams ingestion compleet!")
