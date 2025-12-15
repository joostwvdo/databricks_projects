{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - xG Analysis per Team
    Performance analysis comparing actual goals vs expected goals
*/

WITH matches AS (
    SELECT * FROM {{ ref('fct_matches_with_xg') }}
    WHERE home_xg IS NOT NULL
),

home_stats AS (
    SELECT
        competition_name,
        competition_code,
        home_team_name AS team_name,
        COUNT(*) AS home_matches,
        SUM(home_goals) AS home_goals_scored,
        SUM(away_goals) AS home_goals_conceded,
        SUM(home_xg) AS home_xg,
        SUM(away_xg) AS home_xga,
        SUM(home_points) AS home_points
    FROM matches
    GROUP BY competition_name, competition_code, home_team_name
),

away_stats AS (
    SELECT
        competition_name,
        competition_code,
        away_team_name AS team_name,
        COUNT(*) AS away_matches,
        SUM(away_goals) AS away_goals_scored,
        SUM(home_goals) AS away_goals_conceded,
        SUM(away_xg) AS away_xg,
        SUM(home_xg) AS away_xga,
        SUM(away_points) AS away_points
    FROM matches
    GROUP BY competition_name, competition_code, away_team_name
),

combined AS (
    SELECT
        COALESCE(h.competition_name, a.competition_name) AS competition_name,
        COALESCE(h.competition_code, a.competition_code) AS competition_code,
        COALESCE(h.team_name, a.team_name) AS team_name,

        -- Matches
        COALESCE(h.home_matches, 0) + COALESCE(a.away_matches, 0) AS total_matches,
        COALESCE(h.home_matches, 0) AS home_matches,
        COALESCE(a.away_matches, 0) AS away_matches,

        -- Points
        COALESCE(h.home_points, 0) + COALESCE(a.away_points, 0) AS total_points,

        -- Goals
        COALESCE(h.home_goals_scored, 0) + COALESCE(a.away_goals_scored, 0) AS goals_scored,
        COALESCE(h.home_goals_conceded, 0) + COALESCE(a.away_goals_conceded, 0) AS goals_conceded,

        -- xG
        ROUND(COALESCE(h.home_xg, 0) + COALESCE(a.away_xg, 0), 2) AS total_xg,
        ROUND(COALESCE(h.home_xga, 0) + COALESCE(a.away_xga, 0), 2) AS total_xga

    FROM home_stats h
    FULL OUTER JOIN away_stats a
        ON h.competition_name = a.competition_name
        AND h.team_name = a.team_name
)

SELECT
    competition_name,
    competition_code,
    team_name,
    total_matches,
    total_points,

    -- Goals
    goals_scored,
    goals_conceded,
    goals_scored - goals_conceded AS goal_difference,

    -- xG
    total_xg,
    total_xga,
    ROUND(total_xg - total_xga, 2) AS xg_difference,

    -- Performance vs xG
    ROUND(goals_scored - total_xg, 2) AS goals_vs_xg,
    ROUND(goals_conceded - total_xga, 2) AS goals_conceded_vs_xga,

    -- Per game averages
    ROUND(goals_scored / NULLIF(total_matches, 0), 2) AS goals_per_game,
    ROUND(total_xg / NULLIF(total_matches, 0), 2) AS xg_per_game,
    ROUND(goals_conceded / NULLIF(total_matches, 0), 2) AS goals_conceded_per_game,
    ROUND(total_xga / NULLIF(total_matches, 0), 2) AS xga_per_game,

    -- Luck indicator (positive = outperforming xG)
    ROUND((goals_scored - total_xg) + (total_xga - goals_conceded), 2) AS luck_index,

    -- Finishing quality
    CASE
        WHEN goals_scored > total_xg + 3 THEN 'Elite Finishing'
        WHEN goals_scored > total_xg THEN 'Good Finishing'
        WHEN goals_scored < total_xg - 3 THEN 'Poor Finishing'
        WHEN goals_scored < total_xg THEN 'Below Average Finishing'
        ELSE 'Average Finishing'
    END AS finishing_quality,

    -- Defensive quality
    CASE
        WHEN goals_conceded < total_xga - 3 THEN 'Elite Defense'
        WHEN goals_conceded < total_xga THEN 'Good Defense'
        WHEN goals_conceded > total_xga + 3 THEN 'Poor Defense'
        WHEN goals_conceded > total_xga THEN 'Below Average Defense'
        ELSE 'Average Defense'
    END AS defensive_quality,

    CURRENT_TIMESTAMP() AS transformed_at

FROM combined
WHERE total_matches > 0
ORDER BY competition_name, total_points DESC
