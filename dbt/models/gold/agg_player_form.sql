{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Player Form Analysis
    Laatste 5 wedstrijden statistieken per speler
    Gebaseerd op historische data uit de SCD Type 2 silver layer
*/

WITH current_players AS (
    SELECT * FROM {{ ref('stg_players') }}
    WHERE is_current = TRUE
),

-- Vergelijk met vorige versie voor trend analyse
previous_players AS (
    SELECT
        player_season_key,
        player_name,
        team_name,
        competition_name,
        goals,
        assists,
        xg,
        xa,
        minutes,
        valid_from,
        ROW_NUMBER() OVER (
            PARTITION BY player_season_key
            ORDER BY valid_from DESC
        ) AS version_rank
    FROM {{ ref('stg_players') }}
    WHERE is_current = FALSE
),

-- Pak de vorige versie
prev_version AS (
    SELECT * FROM previous_players WHERE version_rank = 1
),

-- Bereken deltas
player_trends AS (
    SELECT
        c.player_season_key,
        c.player_name,
        c.team_name,
        c.competition_name,
        c.position_category,
        c.nationality,
        c.calculated_age AS age,

        -- Current stats
        c.matches_played,
        c.minutes,
        c.goals,
        c.assists,
        c.goal_contributions,
        c.xg,
        c.xa,
        c.goals_minus_xg,
        c.assists_minus_xa,

        -- Per 90
        c.goals_per90,
        c.assists_per90,
        c.xg_per90,
        c.xa_per90,

        -- Previous stats (if available)
        p.goals AS prev_goals,
        p.assists AS prev_assists,
        p.minutes AS prev_minutes,

        -- Deltas (recent activity)
        c.goals - COALESCE(p.goals, 0) AS goals_delta,
        c.assists - COALESCE(p.assists, 0) AS assists_delta,
        c.minutes - COALESCE(p.minutes, 0) AS minutes_delta,
        c.xg - COALESCE(p.xg, 0) AS xg_delta,
        c.xa - COALESCE(p.xa, 0) AS xa_delta,

        -- Time since last update
        DATEDIFF(c.valid_from, p.valid_from) AS days_since_update,

        c.valid_from AS last_updated,
        c.transformed_at

    FROM current_players c
    LEFT JOIN prev_version p
        ON c.player_season_key = p.player_season_key
)

SELECT
    player_season_key,
    player_name,
    team_name,
    competition_name,
    position_category,
    nationality,
    age,

    -- Current totals
    matches_played,
    minutes,
    goals,
    assists,
    goal_contributions,
    xg,
    xa,
    goals_minus_xg,
    assists_minus_xa,

    -- Per 90
    goals_per90,
    assists_per90,
    xg_per90,
    xa_per90,

    -- Recent activity (deltas)
    goals_delta AS recent_goals,
    assists_delta AS recent_assists,
    minutes_delta AS recent_minutes,
    xg_delta AS recent_xg,
    xa_delta AS recent_xa,

    -- Calculate recent per 90 (if meaningful minutes)
    CASE
        WHEN minutes_delta >= 90 THEN ROUND(goals_delta / (minutes_delta / 90.0), 2)
        ELSE NULL
    END AS recent_goals_per90,

    CASE
        WHEN minutes_delta >= 90 THEN ROUND(assists_delta / (minutes_delta / 90.0), 2)
        ELSE NULL
    END AS recent_assists_per90,

    -- Form indicator
    CASE
        WHEN goals_delta >= 3 OR assists_delta >= 3 THEN '🔥 Hot'
        WHEN goals_delta >= 1 OR assists_delta >= 2 THEN '✅ Good Form'
        WHEN minutes_delta < 90 THEN '🪑 Limited Minutes'
        WHEN goals_delta = 0 AND assists_delta = 0 AND minutes_delta > 180 THEN '📉 Cold'
        ELSE '➡️ Steady'
    END AS current_form,

    -- Hot streak detection
    CASE
        WHEN goals_delta >= 2 AND minutes_delta <= 270 THEN TRUE
        ELSE FALSE
    END AS is_on_hot_streak,

    days_since_update,
    last_updated,
    transformed_at

FROM player_trends
WHERE matches_played >= 3  -- Minimum appearances
ORDER BY competition_name, goal_contributions DESC
