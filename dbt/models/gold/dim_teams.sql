{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Teams Dimension
    Enriched team information with aggregated statistics
*/

WITH teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
),

match_stats AS (
    SELECT * FROM {{ ref('fct_match_results') }}
),

-- Home statistics
home_stats AS (
    SELECT
        home_team_id AS team_id,
        COUNT(*) AS home_matches_played,
        SUM(home_points) AS home_points,
        SUM(home_goals) AS home_goals_scored,
        SUM(away_goals) AS home_goals_conceded,
        SUM(CASE WHEN match_result = 'HOME' THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN is_draw THEN 1 ELSE 0 END) AS home_draws,
        SUM(CASE WHEN match_result = 'AWAY' THEN 1 ELSE 0 END) AS home_losses
    FROM match_stats
    GROUP BY home_team_id
),

-- Away statistics
away_stats AS (
    SELECT
        away_team_id AS team_id,
        COUNT(*) AS away_matches_played,
        SUM(away_points) AS away_points,
        SUM(away_goals) AS away_goals_scored,
        SUM(home_goals) AS away_goals_conceded,
        SUM(CASE WHEN match_result = 'AWAY' THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN is_draw THEN 1 ELSE 0 END) AS away_draws,
        SUM(CASE WHEN match_result = 'HOME' THEN 1 ELSE 0 END) AS away_losses
    FROM match_stats
    GROUP BY away_team_id
),

final AS (
    SELECT
        -- Team info
        t.team_id,
        t.team_name,
        t.short_name,
        t.team_code,
        t.logo_url,
        t.stadium_name,
        t.founded_year,
        t.club_colors,
        t.competition_name,

        -- Total statistics
        COALESCE(h.home_matches_played, 0) + COALESCE(a.away_matches_played, 0) AS total_matches_played,
        COALESCE(h.home_points, 0) + COALESCE(a.away_points, 0) AS total_points,
        COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0) AS total_goals_scored,
        COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0) AS total_goals_conceded,
        COALESCE(h.home_wins, 0) + COALESCE(a.away_wins, 0) AS total_wins,
        COALESCE(h.home_draws, 0) + COALESCE(a.away_draws, 0) AS total_draws,
        COALESCE(h.home_losses, 0) + COALESCE(a.away_losses, 0) AS total_losses,

        -- Goal difference
        (COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0)) -
        (COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0)) AS goal_difference,

        -- Home statistics
        COALESCE(h.home_matches_played, 0) AS home_matches_played,
        COALESCE(h.home_points, 0) AS home_points,
        COALESCE(h.home_wins, 0) AS home_wins,
        COALESCE(h.home_draws, 0) AS home_draws,
        COALESCE(h.home_losses, 0) AS home_losses,

        -- Away statistics
        COALESCE(a.away_matches_played, 0) AS away_matches_played,
        COALESCE(a.away_points, 0) AS away_points,
        COALESCE(a.away_wins, 0) AS away_wins,
        COALESCE(a.away_draws, 0) AS away_draws,
        COALESCE(a.away_losses, 0) AS away_losses,

        -- Averages
        ROUND(
            CAST(COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0) AS DOUBLE) /
            NULLIF(COALESCE(h.home_matches_played, 0) + COALESCE(a.away_matches_played, 0), 0),
            2
        ) AS avg_goals_scored_per_match,

        ROUND(
            CAST(COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0) AS DOUBLE) /
            NULLIF(COALESCE(h.home_matches_played, 0) + COALESCE(a.away_matches_played, 0), 0),
            2
        ) AS avg_goals_conceded_per_match,

        -- Points per game
        ROUND(
            CAST(COALESCE(h.home_points, 0) + COALESCE(a.away_points, 0) AS DOUBLE) /
            NULLIF(COALESCE(h.home_matches_played, 0) + COALESCE(a.away_matches_played, 0), 0),
            2
        ) AS points_per_game,

        -- Metadata
        t.transformed_at

    FROM teams t
    LEFT JOIN home_stats h ON t.team_id = h.team_id
    LEFT JOIN away_stats a ON t.team_id = a.team_id
)

SELECT * FROM final
