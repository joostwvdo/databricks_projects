"""
Fetch football data from FBref (Football Reference) via web scraping.
FBref provides detailed statistics for matches, players, and teams.
"""

import os
import time
import requests
import pandas as pd
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class FBrefClient:
    """Client voor FBref data scraping."""

    # Competition IDs op FBref
    COMPETITIONS = {
        "Eredivisie": {"id": "23", "country": "NED"},
        "Premier League": {"id": "9", "country": "ENG"},
        "La Liga": {"id": "12", "country": "ESP"},
        "Serie A": {"id": "11", "country": "ITA"},
    }

    BASE_URL = "https://fbref.com/en/comps"

    def __init__(self, delay: float = 3.0):
        """
        Initialize FBref client.

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

    def _get_page(self, url: str) -> str:
        """Fetch page content with rate limiting."""
        time.sleep(self.delay)  # Rate limiting
        response = self.session.get(url)
        response.raise_for_status()
        return response.text

    def get_league_table(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal de competitiestand op.

        Args:
            competition: Naam van de competitie (bijv. "Eredivisie")
            season: Seizoen in format "2024-2025"

        Returns:
            DataFrame met de stand
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/{season}-{competition.replace(' ', '-')}-Stats"

        try:
            tables = pd.read_html(url)
            # Eerste tabel is meestal de stand
            if tables:
                df = tables[0]
                df["competition"] = competition
                df["season"] = season
                df["ingest_timestamp"] = datetime.utcnow().isoformat()
                return df
        except Exception as e:
            print(f"Error fetching table for {competition}: {e}")

        return pd.DataFrame()

    def get_match_results(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal wedstrijduitslagen op voor een competitie.

        Args:
            competition: Naam van de competitie
            season: Seizoen

        Returns:
            DataFrame met wedstrijden
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/schedule/{season}-{competition.replace(' ', '-')}-Scores-and-Fixtures"

        try:
            tables = pd.read_html(url)
            if tables:
                df = tables[0]
                df["competition"] = competition
                df["season"] = season
                df["ingest_timestamp"] = datetime.utcnow().isoformat()
                return df
        except Exception as e:
            print(f"Error fetching matches for {competition}: {e}")

        return pd.DataFrame()

    def get_team_stats(self, competition: str, season: str = "2024-2025") -> pd.DataFrame:
        """
        Haal team statistieken op (standard stats).

        Args:
            competition: Naam van de competitie
            season: Seizoen

        Returns:
            DataFrame met team stats
        """
        comp_info = self.COMPETITIONS.get(competition)
        if not comp_info:
            raise ValueError(f"Unknown competition: {competition}")

        comp_id = comp_info["id"]
        url = f"{self.BASE_URL}/{comp_id}/{season}/stats/squads/{season}-{competition.replace(' ', '-')}-Stats"

        try:
            tables = pd.read_html(url)
            # Squad Standard Stats tabel
            for table in tables:
                if "Squad" in str(table.columns):
                    table["competition"] = competition
                    table["season"] = season
                    table["ingest_timestamp"] = datetime.utcnow().isoformat()
                    return table
        except Exception as e:
            print(f"Error fetching team stats for {competition}: {e}")

        return pd.DataFrame()


def transform_fbref_matches(df: pd.DataFrame, competition: str) -> list[dict]:
    """
    Transformeer FBref matches DataFrame naar rijen voor Bronze layer.
    """
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    for _, row in df.iterrows():
        try:
            match_row = {
                "source": "fbref",
                "competition_name": competition,
                "match_date": str(row.get("Date", "")),
                "match_time": str(row.get("Time", "")),
                "home_team_name": str(row.get("Home", "")),
                "away_team_name": str(row.get("Away", "")),
                "home_score": row.get("Score", "").split("–")[0] if pd.notna(row.get("Score")) and "–" in str(row.get("Score")) else None,
                "away_score": row.get("Score", "").split("–")[1] if pd.notna(row.get("Score")) and "–" in str(row.get("Score")) else None,
                "home_xg": row.get("xG", None) if "xG" in df.columns else None,
                "away_xg": row.get("xG.1", None) if "xG.1" in df.columns else None,
                "attendance": row.get("Attendance", None),
                "venue": str(row.get("Venue", "")),
                "referee": str(row.get("Referee", "")),
                "matchweek": row.get("Wk", None),
                "season": row.get("season", ""),
                "ingest_timestamp": ingest_timestamp,
            }
            rows.append(match_row)
        except Exception as e:
            print(f"Error transforming row: {e}")
            continue

    return rows


def transform_fbref_standings(df: pd.DataFrame, competition: str) -> list[dict]:
    """
    Transformeer FBref standings DataFrame naar rijen voor Bronze layer.
    """
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    # Handle multi-level columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(col).strip() for col in df.columns.values]

    for _, row in df.iterrows():
        try:
            standing_row = {
                "source": "fbref",
                "competition_name": competition,
                "position": row.get("Rk", None),
                "team_name": str(row.get("Squad", row.get("Squad_Squad", ""))),
                "matches_played": row.get("MP", row.get("Playing Time_MP", None)),
                "wins": row.get("W", row.get("Playing Time_W", None)),
                "draws": row.get("D", row.get("Playing Time_D", None)),
                "losses": row.get("L", row.get("Playing Time_L", None)),
                "goals_for": row.get("GF", row.get("Performance_GF", None)),
                "goals_against": row.get("GA", row.get("Performance_GA", None)),
                "goal_difference": row.get("GD", row.get("Performance_GD", None)),
                "points": row.get("Pts", row.get("Playing Time_Pts", None)),
                "xg": row.get("xG", row.get("Expected_xG", None)),
                "xga": row.get("xGA", row.get("Expected_xGA", None)),
                "season": row.get("season", ""),
                "ingest_timestamp": ingest_timestamp,
            }
            rows.append(standing_row)
        except Exception as e:
            print(f"Error transforming standing row: {e}")
            continue

    return rows


if __name__ == "__main__":
    # Test de client
    client = FBrefClient()

    competitions = ["Eredivisie", "Premier League", "La Liga", "Serie A"]

    for comp in competitions:
        print(f"\n{'='*50}")
        print(f"Fetching data for {comp}...")

        try:
            # Standings
            standings = client.get_league_table(comp)
            if not standings.empty:
                print(f"  - Found {len(standings)} teams in standings")
                print(f"  - Columns: {list(standings.columns)[:5]}...")

            # Matches
            matches = client.get_match_results(comp)
            if not matches.empty:
                print(f"  - Found {len(matches)} matches")

        except Exception as e:
            print(f"  - Error: {e}")

        # Rate limiting between competitions
        time.sleep(3)
