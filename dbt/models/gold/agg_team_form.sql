{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Recent Form Analysis
    Laatste 5 wedstrijden per team voor vorm analyse
*/

WITH matches AS (
    SELECT
        match_date,
        competition_name,
        competition_code,
        home_team_name,
        away_team_name,
        home_goals,
        away_goals,
        match_result,
        home_xg,
        away_xg
    FROM {{ ref('stg_fbref_matches') }}
    WHERE is_current = TRUE
      AND is_finished = TRUE
),

-- Alle resultaten per team
team_results AS (
    -- Home matches
    SELECT
        home_team_name AS team_name,
        competition_name,
        match_date,
        home_goals AS goals_scored,
        away_goals AS goals_conceded,
        home_xg AS xg,
        away_xg AS xga,
        CASE
            WHEN match_result = 'HOME' THEN 'W'
            WHEN match_result = 'DRAW' THEN 'D'
            ELSE 'L'
        END AS result,
        CASE
            WHEN match_result = 'HOME' THEN 3
            WHEN match_result = 'DRAW' THEN 1
            ELSE 0
        END AS points,
        'H' AS venue,
        away_team_name AS opponent
    FROM matches

    UNION ALL

    -- Away matches
    SELECT
        away_team_name AS team_name,
        competition_name,
        match_date,
        away_goals AS goals_scored,
        home_goals AS goals_conceded,
        away_xg AS xg,
        home_xg AS xga,
        CASE
            WHEN match_result = 'AWAY' THEN 'W'
            WHEN match_result = 'DRAW' THEN 'D'
            ELSE 'L'
        END AS result,
        CASE
            WHEN match_result = 'AWAY' THEN 3
            WHEN match_result = 'DRAW' THEN 1
            ELSE 0
        END AS points,
        'A' AS venue,
        home_team_name AS opponent
    FROM matches
),

-- Rank matches per team (most recent first)
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY team_name, competition_name
            ORDER BY match_date DESC
        ) AS match_recency
    FROM team_results
),

-- Laatste 5 wedstrijden
last_5 AS (
    SELECT * FROM ranked WHERE match_recency <= 5
),

-- Form string en stats
form_stats AS (
    SELECT
        team_name,
        competition_name,

        -- Form string (meest recent eerst): bijv. "WWDLW"
        CONCAT_WS('', COLLECT_LIST(result)) AS form_string,

        -- Stats over laatste 5
        COUNT(*) AS matches_in_form,
        SUM(points) AS form_points,
        SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS form_wins,
        SUM(CASE WHEN result = 'D' THEN 1 ELSE 0 END) AS form_draws,
        SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS form_losses,

        SUM(goals_scored) AS form_goals_scored,
        SUM(goals_conceded) AS form_goals_conceded,
        ROUND(AVG(goals_scored), 2) AS form_avg_goals_scored,
        ROUND(AVG(goals_conceded), 2) AS form_avg_goals_conceded,

        ROUND(SUM(xg), 2) AS form_xg,
        ROUND(SUM(xga), 2) AS form_xga,

        -- Clean sheets & scoring
        SUM(CASE WHEN goals_conceded = 0 THEN 1 ELSE 0 END) AS form_clean_sheets,
        SUM(CASE WHEN goals_scored = 0 THEN 1 ELSE 0 END) AS form_failed_to_score,

        MIN(match_date) AS form_period_start,
        MAX(match_date) AS form_period_end

    FROM last_5
    GROUP BY team_name, competition_name
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_name', 'competition_name']) }} AS form_key,

    team_name,
    competition_name,

    form_string,
    matches_in_form,

    -- Points from last 5
    form_points,
    ROUND(form_points / matches_in_form, 2) AS form_ppg,

    -- W/D/L
    form_wins,
    form_draws,
    form_losses,

    -- Goals
    form_goals_scored,
    form_goals_conceded,
    form_goals_scored - form_goals_conceded AS form_goal_diff,
    form_avg_goals_scored,
    form_avg_goals_conceded,

    -- xG
    form_xg,
    form_xga,
    ROUND(form_xg - form_xga, 2) AS form_xg_diff,

    -- Clean sheets
    form_clean_sheets,
    form_failed_to_score,

    -- Period
    form_period_start,
    form_period_end,

    -- Form rating (max 15 punten uit 5 wedstrijden)
    CASE
        WHEN form_points >= 13 THEN '🔥 Excellent'
        WHEN form_points >= 10 THEN '✅ Good'
        WHEN form_points >= 7 THEN '➡️ Average'
        WHEN form_points >= 4 THEN '⚠️ Poor'
        ELSE '❌ Very Poor'
    END AS form_rating,

    -- Trend indicator
    CASE
        WHEN form_wins >= 4 THEN 'Hot Streak'
        WHEN form_losses >= 4 THEN 'Cold Streak'
        WHEN form_draws >= 3 THEN 'Drawing Habit'
        ELSE 'Mixed'
    END AS form_trend,

    CURRENT_TIMESTAMP() AS transformed_at

FROM form_stats
ORDER BY competition_name, form_points DESC
