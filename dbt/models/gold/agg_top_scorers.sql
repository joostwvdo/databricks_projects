{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Top Scorers
    Ranking of top goal scorers per competition
*/

WITH players AS (
    SELECT * FROM {{ ref('fct_player_performance') }}
),

ranked AS (
    SELECT
        -- Ranking
        ROW_NUMBER() OVER (
            PARTITION BY competition_name
            ORDER BY goals DESC, assists DESC, minutes ASC
        ) AS rank_in_competition,

        ROW_NUMBER() OVER (
            ORDER BY goals DESC, assists DESC, minutes ASC
        ) AS overall_rank,

        -- Player info
        player_name,
        team_name,
        nationality,
        position,
        position_category,
        age,

        -- Competition
        competition_name,
        competition_code,
        season,

        -- Playing time
        matches_played,
        starts,
        minutes,
        nineties,

        -- Goals
        goals,
        non_penalty_goals,
        penalty_goals,
        goals_per90,

        -- xG comparison
        xg,
        goals_minus_xg,

        -- Efficiency
        shots,
        shots_on_target,
        shot_conversion_pct,
        shots_per_goal,

        -- Assists (secondary)
        assists,
        goal_contributions,

        -- Minutes per goal
        CASE
            WHEN goals > 0 THEN ROUND(minutes / goals, 0)
            ELSE NULL
        END AS minutes_per_goal,

        -- Goal contribution percentage of team (needs team goals)
        transformed_at

    FROM players
    WHERE goals > 0
)

SELECT
    *,

    -- Golden Boot race indicator
    CASE
        WHEN rank_in_competition = 1 THEN '🥇 Leader'
        WHEN rank_in_competition <= 3 THEN '🥈 Contender'
        WHEN rank_in_competition <= 5 THEN '🥉 In Race'
        ELSE 'Chasing'
    END AS golden_boot_status

FROM ranked
ORDER BY competition_name, rank_in_competition
