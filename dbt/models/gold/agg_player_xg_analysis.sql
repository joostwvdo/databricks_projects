{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Player xG Analysis
    Detailed expected goals analysis per player
*/

WITH players AS (
    SELECT * FROM {{ ref('fct_player_performance') }}
    WHERE minutes >= 450  -- Minimum 5 full matches
),

xg_analysis AS (
    SELECT
        -- Player info
        player_name,
        team_name,
        position,
        position_category,
        nationality,
        age,

        -- Competition
        competition_name,
        competition_code,
        season,

        -- Playing time
        matches_played,
        minutes,
        nineties,

        -- Actual vs Expected Goals
        goals,
        xg,
        npxg,
        goals_minus_xg,

        -- Actual vs Expected Assists
        assists,
        xa,
        assists_minus_xa,

        -- Combined
        goal_contributions,
        xg_xa,
        goals_minus_xg + assists_minus_xa AS total_overperformance,

        -- Per 90 metrics
        goals_per90,
        xg_per90,
        assists_per90,
        xa_per90,
        goal_contributions_per90,
        xg_xa_per90,

        -- Overperformance per 90
        ROUND((goals - xg) / NULLIF(nineties, 0), 2) AS goals_over_xg_per90,
        ROUND((assists - xa) / NULLIF(nineties, 0), 2) AS assists_over_xa_per90,

        -- Shooting context
        shots,
        shots_on_target,
        shot_conversion_pct,

        -- xG per shot (shot quality)
        CASE
            WHEN shots > 0 THEN ROUND(xg / shots, 3)
            ELSE NULL
        END AS xg_per_shot,

        -- Goals per xG (finishing skill)
        CASE
            WHEN xg > 0 THEN ROUND(goals / xg, 2)
            ELSE NULL
        END AS goals_per_xg,

        -- Classification
        CASE
            WHEN goals - xg >= 5 THEN 'Elite Finisher'
            WHEN goals - xg >= 2 THEN 'Clinical'
            WHEN goals - xg >= 0 THEN 'On Par'
            WHEN goals - xg >= -2 THEN 'Slightly Wasteful'
            ELSE 'Underperforming'
        END AS finishing_category,

        CASE
            WHEN assists - xa >= 3 THEN 'Elite Creator'
            WHEN assists - xa >= 1 THEN 'Quality Provider'
            WHEN assists - xa >= 0 THEN 'On Par'
            ELSE 'Underperforming'
        END AS creative_category,

        -- Luck/Skill index (positive = skill/form, negative = unlucky/poor form)
        ROUND(
            ((goals - xg) / NULLIF(xg, 0) * 50) +
            ((assists - xa) / NULLIF(xa, 0) * 50),
            1
        ) AS skill_luck_index,

        transformed_at

    FROM players
    WHERE xg > 0 OR xa > 0  -- Must have some expected contribution
)

SELECT
    *,

    -- Rankings within competition
    ROW_NUMBER() OVER (
        PARTITION BY competition_name
        ORDER BY goals_minus_xg DESC
    ) AS finishing_rank,

    ROW_NUMBER() OVER (
        PARTITION BY competition_name
        ORDER BY assists_minus_xa DESC
    ) AS creating_rank,

    ROW_NUMBER() OVER (
        PARTITION BY competition_name
        ORDER BY total_overperformance DESC
    ) AS overall_overperformance_rank

FROM xg_analysis
ORDER BY competition_name, total_overperformance DESC
