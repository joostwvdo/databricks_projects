# Databricks Voetbal Data Project

Dit project haalt voetbalwedstrijden op en verwerkt deze via een medallion architectuur (Bronze → Silver → Gold) in Databricks.

## 🚀 Quick Start in Databricks

### 1. Open Databricks Workspace
Ga naar: https://dbc-63b440bb-027b.cloud.databricks.com/

### 2. Navigeer naar de notebooks
📁 **Locatie**: `/Shared/football_data/`

### 3. Start een cluster
- Ga naar **Compute** → Start je cluster
- Wacht tot status "Running" is

### 4. Run de pipeline
Open `00_run_all` en klik **Run All** - dit voert automatisch uit:
1. Setup catalog en schemas
2. Bronze: Data ophalen van API
3. Silver: Transformaties
4. Gold: Aggregaties

## 📁 Project Structuur

```
databricks/
├── notebooks/              # Databricks notebooks
│   └── bronze/            # Data ingestion notebooks
├── dbt/                   # DBT project voor transformaties
│   ├── models/
│   │   ├── silver/       # Cleaned & transformed data (SCD Type 2)
│   │   └── gold/         # Business-ready aggregaties
│   ├── macros/           # Herbruikbare SQL macros
│   └── ...
├── src/                   # Python source code
│   └── ingestion/        # Data ophaal scripts
└── config/               # Configuratie bestanden
```

## 🏗️ Architectuur

### Bronze Layer
- Ruwe voetbaldata van externe API's (bijv. football-data.org)
- Data wordt opgeslagen als-is in Delta tables
- Bevat metadata zoals ingest_timestamp

### Silver Layer (DBT) - SCD Type 2
- Gecleande en gevalideerde data
- **Historische tracking met SCD Type 2**:
  - `valid_from` - Start timestamp van record versie
  - `valid_to` - Eind timestamp (NULL = huidige versie)
  - `is_current` - Flag voor actieve records
  - `record_hash` - MD5 hash voor change detection
- Gestandaardiseerde kolomnamen
- Deduplicatie en data quality checks

### Gold Layer (DBT)
- Business-ready aggregaties
- Team statistieken en analyses
- Competitie standings
- Head-to-head analyses
- Vorm analyses (laatste 5 wedstrijden)
- Historische positie tracking

## 🔄 SCD Type 2 Implementatie

De Silver layer implementeert Slowly Changing Dimensions Type 2 voor volledige historie:

```sql
-- Voorbeeld: Huidige data ophalen
SELECT * FROM silver.stg_matches WHERE is_current = TRUE;

-- Voorbeeld: Historische staat op specifieke datum
SELECT * FROM silver.stg_players 
WHERE valid_from <= '2024-01-15' 
  AND (valid_to > '2024-01-15' OR valid_to IS NULL);

-- Voorbeeld: Alle versies van een record
SELECT * FROM silver.stg_fbref_standings 
WHERE team_name = 'Ajax'
ORDER BY valid_from;
```

### Change Detection
Elke tabel heeft een `record_hash` kolom (MD5 hash van relevante velden). Bij een incremental run:
1. Nieuwe records met andere hash → Oude record krijgt `valid_to` en `is_current = FALSE`
2. Nieuwe versie wordt toegevoegd met `is_current = TRUE`

## 🚀 Setup

### Vereisten
- Databricks workspace
- Python 3.9+
- DBT Core met dbt-databricks adapter

### Installatie

```bash
# Virtuele omgeving aanmaken
python -m venv .venv
source .venv/bin/activate

# Dependencies installeren
pip install -r requirements.txt

# DBT dependencies installeren
cd dbt
dbt deps
```

### Configuratie

1. Kopieer `.env.example` naar `.env` en vul je credentials in
2. Configureer je Databricks connection in `dbt/profiles.yml`

## 📊 Data Sources

- **Football-Data.org API**: Gratis API voor voetbalcompetities en wedstrijden
- **FBref (Football Reference)**: Gedetailleerde statistieken inclusief xG (Expected Goals)

## 🏆 Competities

| Competitie | Code | Land |
|------------|------|------|
| 🇳🇱 Eredivisie | DED | Nederland |
| 🏴󠁧󠁢󠁥󠁮󠁧󠁿 Premier League | PL | Engeland |
| 🇪🇸 La Liga | PD | Spanje |
| 🇮🇹 Serie A | SA | Italië |

## 🔄 Data Pipeline

1. **Ingestion (Bronze)**: 
   - `01_ingest_matches.py` - Football-Data.org API
   - `02_ingest_fbref.py` - FBref web scraping (met xG data)
   - `03_ingest_players.py` - FBref speler statistieken
2. **Transformation (Silver)**: DBT models cleanen de data (SCD Type 2)
   - `stg_matches` - API wedstrijden met historie
   - `stg_teams` - Team info met historie
   - `stg_fbref_matches` - FBref wedstrijden met xG
   - `stg_fbref_standings` - FBref standen met xG
   - `stg_players` - Speler statistieken met historie
3. **Aggregation (Gold)**: DBT models maken business views
   - `fct_match_results` - Wedstrijd feiten
   - `fct_matches_with_xg` - Gecombineerde data met xG
   - `fct_player_performance` - Speler prestaties per seizoen
   - `dim_teams` - Team dimensie
   - `dim_players` - Speler dimensie met profielen
   - `agg_competition_standings` - Competitie standen
   - `agg_team_xg_analysis` - xG analyse per team
   - `agg_top_scorers` - Topscorers ranking
   - `agg_top_assisters` - Top assistgevers
   - `agg_player_xg_analysis` - Speler xG analyse
   - `agg_head_to_head` - Onderlinge resultaten tussen teams
   - `agg_team_form` - Laatste 5 wedstrijden analyse
   - `agg_player_form` - Speler vorm analyse (delta's)
   - `agg_standing_history` - Historische positie beweging

## 📝 Gebruik

```bash
# Bronze data ophalen
python src/ingestion/fetch_matches.py

# DBT transformaties uitvoeren
cd dbt
dbt run

# Alleen silver layer
dbt run --select silver.*

# Alleen gold layer  
dbt run --select gold.*

# Specifiek model met dependencies
dbt run --select +agg_head_to_head
```

## 📈 Analyse Voorbeelden

### Head-to-Head Analyse
```sql
SELECT * FROM gold.agg_head_to_head
WHERE team_a = 'Ajax' AND team_b = 'Feyenoord';
```

### Team Vorm
```sql
SELECT team_name, form_string, form_points, form_rating
FROM gold.agg_team_form
WHERE competition_name = 'Eredivisie'
ORDER BY form_points DESC;
```

### Speler Hot Streaks
```sql
SELECT player_name, team_name, recent_goals, current_form
FROM gold.agg_player_form
WHERE is_on_hot_streak = TRUE;
```

### Historische Stand Beweging
```sql
SELECT team_name, snapshot_date, position, position_movement
FROM gold.agg_standing_history
WHERE team_name = 'PSV'
ORDER BY snapshot_date;
```

## 📄 Licentie

MIT License
