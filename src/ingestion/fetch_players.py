"""
Fetch player statistics from FBref (Football Reference).
Includes detailed player stats like goals, assists, xG, xA, minutes played, etc.
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class FBrefPlayerClient:
    """Client voor FBref player statistics scraping."""

    # Competition IDs op FBref
    COMPETITIONS = {
        "Eredivisie": {"id": "23", "country": "NED"},
        "Premier League": {"id": "9", "country": "ENG"},
        "La Liga": {"id": "12", "country": "ESP"},
        "Serie A": {"id": "11", "country": "ITA"},
    }

    BASE_URL = "https://fbref.com/en/comps"

    def __init__(self, delay: float = 4.0):
        """
        Initialize FBref player client.

        Args:
            delay: Delay between requests (FBref rate limits aggressively)
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36"
        })

    def _get_tables(self, url: str) -> list[pd.DataFrame]:
        """Fetch tables from URL with rate limiting."""
        time.sleep(self.delay)
        try:
            tables = pd.read_html(url)
            return tables
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return []

    def get_player_standard_stats(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal standaard speler statistieken op.
        Inclusief: goals, assists, minutes, starts, etc.
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/stats/players/{season}-{competition.replace(' ', '-')}-Stats"

        print(f"Fetching standard stats: {url}")
        tables = self._get_tables(url)

        for table in tables:
            # Look for the main player stats table
            cols = [str(c) for c in table.columns.tolist()]
            if any("Player" in str(c) for c in cols) and any("Gls" in str(c) or "Goals" in str(c) for c in cols):
                table["competition"] = competition
                table["season"] = season
                table["stat_type"] = "standard"
                return table

        return pd.DataFrame()

    def get_player_shooting_stats(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal shooting statistieken op.
        Inclusief: shots, shots on target, xG, npxG, etc.
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/shooting/players/{season}-{competition.replace(' ', '-')}-Stats"

        print(f"Fetching shooting stats: {url}")
        tables = self._get_tables(url)

        for table in tables:
            cols = [str(c) for c in table.columns.tolist()]
            if any("Player" in str(c) for c in cols) and any("Sh" in str(c) or "Shot" in str(c) for c in cols):
                table["competition"] = competition
                table["season"] = season
                table["stat_type"] = "shooting"
                return table

        return pd.DataFrame()

    def get_player_passing_stats(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal passing statistieken op.
        Inclusief: passes completed, pass %, key passes, xA, etc.
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/passing/players/{season}-{competition.replace(' ', '-')}-Stats"

        print(f"Fetching passing stats: {url}")
        tables = self._get_tables(url)

        for table in tables:
            cols = [str(c) for c in table.columns.tolist()]
            if any("Player" in str(c) for c in cols) and any("Cmp" in str(c) or "Pass" in str(c) for c in cols):
                table["competition"] = competition
                table["season"] = season
                table["stat_type"] = "passing"
                return table

        return pd.DataFrame()

    def get_player_defensive_stats(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal defensive statistieken op.
        Inclusief: tackles, interceptions, blocks, clearances, etc.
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/defense/players/{season}-{competition.replace(' ', '-')}-Stats"

        print(f"Fetching defensive stats: {url}")
        tables = self._get_tables(url)

        for table in tables:
            cols = [str(c) for c in table.columns.tolist()]
            if any("Player" in str(c) for c in cols) and any("Tkl" in str(c) or "Tackle" in str(c) for c in cols):
                table["competition"] = competition
                table["season"] = season
                table["stat_type"] = "defense"
                return table

        return pd.DataFrame()

    def get_player_gca_stats(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal Goal and Shot Creating Actions statistieken op.
        Inclusief: SCA, GCA, etc.
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/gca/players/{season}-{competition.replace(' ', '-')}-Stats"

        print(f"Fetching GCA stats: {url}")
        tables = self._get_tables(url)

        for table in tables:
            cols = [str(c) for c in table.columns.tolist()]
            if any("Player" in str(c) for c in cols) and any("SCA" in str(c) or "GCA" in str(c) for c in cols):
                table["competition"] = competition
                table["season"] = season
                table["stat_type"] = "gca"
                return table

        return pd.DataFrame()


def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten multi-level column names."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(str(c) for c in col).strip() for col in df.columns.values]
    return df


def transform_player_stats(df: pd.DataFrame, competition: str, stat_type: str) -> list[dict]:
    """
    Transform player stats DataFrame to flat rows for Bronze layer.
    """
    if df.empty:
        return []

    df = flatten_columns(df.copy())
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    # Common column mappings (FBref uses various naming conventions)
    player_col = next((c for c in df.columns if 'Player' in c), None)
    nation_col = next((c for c in df.columns if 'Nation' in c), None)
    pos_col = next((c for c in df.columns if 'Pos' in c), None)
    squad_col = next((c for c in df.columns if 'Squad' in c), None)
    age_col = next((c for c in df.columns if 'Age' in c), None)
    born_col = next((c for c in df.columns if 'Born' in c), None)
    mp_col = next((c for c in df.columns if c in ['MP', 'Matches_MP', 'Playing Time_MP']), None)
    min_col = next((c for c in df.columns if c in ['Min', 'Matches_Min', 'Playing Time_Min', '90s']), None)

    for _, row in df.iterrows():
        try:
            player_name = str(row.get(player_col, "")) if player_col else ""
            if not player_name or player_name == "nan":
                continue

            base_row = {
                "source": "fbref",
                "stat_type": stat_type,
                "competition_name": competition,
                "season": str(row.get("season", "")),
                "player_name": player_name,
                "nationality": str(row.get(nation_col, "")) if nation_col else None,
                "position": str(row.get(pos_col, "")) if pos_col else None,
                "team_name": str(row.get(squad_col, "")) if squad_col else None,
                "age": str(row.get(age_col, "")) if age_col else None,
                "birth_year": int(row.get(born_col)) if born_col and pd.notna(row.get(born_col)) else None,
                "matches_played": int(row.get(mp_col)) if mp_col and pd.notna(row.get(mp_col)) else None,
                "minutes": int(row.get(min_col)) if min_col and pd.notna(row.get(min_col)) else None,
                "ingest_timestamp": ingest_timestamp,
            }

            # Add all numeric columns as stats
            for col in df.columns:
                if col not in [player_col, nation_col, pos_col, squad_col, age_col, born_col, "competition", "season", "stat_type"]:
                    try:
                        val = row.get(col)
                        if pd.notna(val):
                            # Clean column name
                            clean_col = col.lower().replace(" ", "_").replace("-", "_")
                            base_row[f"stat_{clean_col}"] = float(val) if isinstance(val, (int, float)) else str(val)
                    except:
                        pass

            rows.append(base_row)
        except Exception as e:
            continue

    return rows


if __name__ == "__main__":
    client = FBrefPlayerClient()

    competitions = ["Eredivisie", "Premier League", "La Liga", "Serie A"]

    for comp in competitions[:1]:  # Test with first competition
        print(f"\n{'='*60}")
        print(f"🏆 Fetching player stats for {comp}...")

        try:
            # Standard stats
            standard = client.get_player_standard_stats(comp)
            if not standard.empty:
                print(f"  ✅ Standard stats: {len(standard)} players")
                rows = transform_player_stats(standard, comp, "standard")
                if rows:
                    print(f"     Sample: {rows[0].get('player_name')} - {rows[0].get('team_name')}")

            # Shooting stats
            shooting = client.get_player_shooting_stats(comp)
            if not shooting.empty:
                print(f"  ✅ Shooting stats: {len(shooting)} players")

        except Exception as e:
            print(f"  ❌ Error: {e}")
