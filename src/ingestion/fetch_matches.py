"""
Fetch football matches from Football-Data.org API and store in Bronze layer.
"""

import os
import requests
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


class FootballDataClient:
    """Client voor Football-Data.org API."""

    # Competition codes voor de gewenste leagues
    COMPETITIONS = {
        "Eredivisie": "DED",      # Dutch Eredivisie
        "Premier League": "PL",   # English Premier League
        "La Liga": "PD",          # Spanish La Liga (Primera División)
        "Serie A": "SA",          # Italian Serie A
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("FOOTBALL_API_KEY")
        self.base_url = os.getenv(
            "FOOTBALL_API_BASE_URL", "https://api.football-data.org/v4"
        )
        self.headers = {"X-Auth-Token": self.api_key}

    def get_competitions(self) -> dict:
        """Haal alle beschikbare competities op."""
        response = requests.get(
            f"{self.base_url}/competitions", headers=self.headers
        )
        response.raise_for_status()
        return response.json()

    def get_matches(
        self,
        competition_code: str = "PL",
        season: Optional[int] = None,
        matchday: Optional[int] = None,
    ) -> dict:
        """
        Haal wedstrijden op voor een specifieke competitie.

        Args:
            competition_code: Code van de competitie (bijv. 'PL' voor Premier League,
                            'BL1' voor Bundesliga, 'SA' voor Serie A, 'PD' voor La Liga)
            season: Seizoen jaar (bijv. 2024 voor 2024/25)
            matchday: Specifieke speelronde

        Returns:
            Dict met wedstrijden data
        """
        params = {}
        if season:
            params["season"] = season
        if matchday:
            params["matchday"] = matchday

        response = requests.get(
            f"{self.base_url}/competitions/{competition_code}/matches",
            headers=self.headers,
            params=params,
        )
        response.raise_for_status()
        return response.json()

    def get_teams(self, competition_code: str = "PL", season: Optional[int] = None) -> dict:
        """Haal teams op voor een competitie."""
        params = {}
        if season:
            params["season"] = season

        response = requests.get(
            f"{self.base_url}/competitions/{competition_code}/teams",
            headers=self.headers,
            params=params,
        )
        response.raise_for_status()
        return response.json()

    def get_standings(self, competition_code: str = "PL", season: Optional[int] = None) -> dict:
        """Haal de stand op voor een competitie."""
        params = {}
        if season:
            params["season"] = season

        response = requests.get(
            f"{self.base_url}/competitions/{competition_code}/standings",
            headers=self.headers,
            params=params,
        )
        response.raise_for_status()
        return response.json()


def transform_matches_to_rows(matches_data: dict) -> list[dict]:
    """
    Transformeer API response naar platte rijen voor Bronze layer.
    """
    rows = []
    ingest_timestamp = datetime.utcnow().isoformat()

    competition = matches_data.get("competition", {})

    for match in matches_data.get("matches", []):
        row = {
            # Match info
            "match_id": match.get("id"),
            "utc_date": match.get("utcDate"),
            "status": match.get("status"),
            "matchday": match.get("matchday"),
            "stage": match.get("stage"),
            "last_updated": match.get("lastUpdated"),
            # Competition info
            "competition_id": competition.get("id"),
            "competition_name": competition.get("name"),
            "competition_code": competition.get("code"),
            # Home team
            "home_team_id": match.get("homeTeam", {}).get("id"),
            "home_team_name": match.get("homeTeam", {}).get("name"),
            "home_team_short_name": match.get("homeTeam", {}).get("shortName"),
            # Away team
            "away_team_id": match.get("awayTeam", {}).get("id"),
            "away_team_name": match.get("awayTeam", {}).get("name"),
            "away_team_short_name": match.get("awayTeam", {}).get("shortName"),
            # Score
            "home_score_fulltime": match.get("score", {}).get("fullTime", {}).get("home"),
            "away_score_fulltime": match.get("score", {}).get("fullTime", {}).get("away"),
            "home_score_halftime": match.get("score", {}).get("halfTime", {}).get("home"),
            "away_score_halftime": match.get("score", {}).get("halfTime", {}).get("away"),
            "winner": match.get("score", {}).get("winner"),
            # Metadata
            "ingest_timestamp": ingest_timestamp,
        }
        rows.append(row)

    return rows


def transform_teams_to_rows(teams_data: dict) -> list[dict]:
    """Transformeer teams API response naar platte rijen."""
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


if __name__ == "__main__":
    # Test de client
    client = FootballDataClient()

    print("Fetching competitions...")
    competitions = client.get_competitions()
    print(f"Found {len(competitions.get('competitions', []))} competitions")

    # Fetch data voor alle gewenste competities
    for league_name, league_code in client.COMPETITIONS.items():
        print(f"\n{'='*50}")
        print(f"Fetching {league_name} ({league_code}) matches...")

        try:
            matches = client.get_matches(competition_code=league_code)
            rows = transform_matches_to_rows(matches)
            print(f"Found {len(rows)} matches")

            if rows:
                print(f"Sample: {rows[0].get('home_team_name')} vs {rows[0].get('away_team_name')}")
        except Exception as e:
            print(f"Error: {e}")
