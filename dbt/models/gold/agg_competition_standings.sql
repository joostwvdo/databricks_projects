{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Competition Standings
    Current standings per competition based on match results
*/

WITH team_stats AS (
    SELECT * FROM {{ ref('dim_teams') }}
),

ranked AS (
    SELECT
        -- Position
        ROW_NUMBER() OVER (
            PARTITION BY competition_name
            ORDER BY total_points DESC, goal_difference DESC, total_goals_scored DESC
        ) AS position,

        -- Team info
        team_id,
        team_name,
        team_code,
        competition_name,

        -- Statistics
        total_matches_played AS played,
        total_wins AS won,
        total_draws AS drawn,
        total_losses AS lost,
        total_goals_scored AS goals_for,
        total_goals_conceded AS goals_against,
        goal_difference,
        total_points AS points,

        -- Form indicators
        points_per_game,
        avg_goals_scored_per_match,
        avg_goals_conceded_per_match,

        -- Home/Away breakdown
        home_wins,
        home_draws,
        home_losses,
        away_wins,
        away_draws,
        away_losses,

        -- Metadata
        transformed_at

    FROM team_stats
    WHERE total_matches_played > 0
)

SELECT
    *,

    -- Zone indicators (typical for top 5 leagues)
    CASE
        WHEN position <= 4 THEN 'Champions League'
        WHEN position <= 6 THEN 'Europa League'
        WHEN position >= (SELECT MAX(position) - 2 FROM ranked r2 WHERE r2.competition_name = ranked.competition_name)
            THEN 'Relegation Zone'
        ELSE 'Mid-table'
    END AS zone

FROM ranked
ORDER BY competition_name, position
