{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Matchday Summary
    Aggregated statistics per matchday per competition
*/

WITH matches AS (
    SELECT * FROM {{ ref('fct_match_results') }}
),

matchday_stats AS (
    SELECT
        competition_code,
        competition_name,
        matchday_number,
        MIN(match_date) AS matchday_start_date,
        MAX(match_date) AS matchday_end_date,

        -- Match counts
        COUNT(*) AS total_matches,

        -- Goals
        SUM(total_goals) AS total_goals,
        ROUND(AVG(total_goals), 2) AS avg_goals_per_match,
        MAX(total_goals) AS max_goals_in_match,

        -- Results breakdown
        SUM(CASE WHEN match_result = 'HOME' THEN 1 ELSE 0 END) AS home_wins,
        SUM(CASE WHEN match_result = 'AWAY' THEN 1 ELSE 0 END) AS away_wins,
        SUM(CASE WHEN is_draw THEN 1 ELSE 0 END) AS draws,

        -- Percentages
        ROUND(
            100.0 * SUM(CASE WHEN match_result = 'HOME' THEN 1 ELSE 0 END) / COUNT(*),
            1
        ) AS home_win_pct,
        ROUND(
            100.0 * SUM(CASE WHEN match_result = 'AWAY' THEN 1 ELSE 0 END) / COUNT(*),
            1
        ) AS away_win_pct,
        ROUND(
            100.0 * SUM(CASE WHEN is_draw THEN 1 ELSE 0 END) / COUNT(*),
            1
        ) AS draw_pct,

        -- Special matches
        SUM(CASE WHEN is_high_scoring THEN 1 ELSE 0 END) AS high_scoring_matches,
        SUM(CASE WHEN is_goalless THEN 1 ELSE 0 END) AS goalless_draws,
        SUM(CASE WHEN both_teams_scored THEN 1 ELSE 0 END) AS both_teams_scored_matches,

        -- BTTS percentage
        ROUND(
            100.0 * SUM(CASE WHEN both_teams_scored THEN 1 ELSE 0 END) / COUNT(*),
            1
        ) AS btts_pct,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM matches
    GROUP BY
        competition_code,
        competition_name,
        matchday_number
)

SELECT * FROM matchday_stats
ORDER BY competition_name, matchday_number
