{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Player Performance Facts
    Detailed player performance metrics per season/competition
*/

WITH players AS (
    SELECT * FROM {{ ref('stg_players') }}
    WHERE minutes >= 90  -- Minimum playing time filter
),

teams AS (
    SELECT DISTINCT
        team_name,
        competition_name,
        competition_code
    FROM {{ ref('stg_teams') }}
),

final AS (
    SELECT
        -- Keys
        p.player_season_key,
        {{ dbt_utils.generate_surrogate_key(['p.player_name']) }} AS player_key,

        -- Player info
        p.player_name,
        p.team_name,
        p.position,
        p.position_category,
        p.nationality,
        p.calculated_age AS age,

        -- Competition
        p.competition_name,
        p.competition_code,
        p.season,

        -- Playing time
        p.matches_played,
        p.starts,
        p.minutes,
        p.nineties,
        p.sub_appearances,

        -- Calculate minutes per start
        CASE
            WHEN p.starts > 0 THEN ROUND(p.minutes / p.starts, 0)
            ELSE NULL
        END AS minutes_per_start,

        -- Goals & Assists
        p.goals,
        p.assists,
        p.goal_contributions,
        p.non_penalty_goals,
        p.penalty_goals,

        -- xG Stats
        p.xg,
        p.npxg,
        p.xa,
        p.xg_xa,

        -- Performance vs Expected
        p.goals_minus_xg,
        p.assists_minus_xa,
        p.goals_minus_xg + p.assists_minus_xa AS total_overperformance,

        -- Per 90 metrics
        p.goals_per90,
        p.assists_per90,
        ROUND((p.goals + p.assists) / NULLIF(p.nineties, 0), 2) AS goal_contributions_per90,
        p.xg_per90,
        p.xa_per90,
        ROUND((p.xg + p.xa) / NULLIF(p.nineties, 0), 2) AS xg_xa_per90,

        -- Shooting efficiency
        p.shots,
        p.shots_on_target,
        p.shot_accuracy_pct,
        p.shot_conversion_pct,
        p.shots_per90,

        -- Shots per goal
        CASE
            WHEN p.goals > 0 THEN ROUND(p.shots / p.goals, 1)
            ELSE NULL
        END AS shots_per_goal,

        -- Passing
        p.passes_completed,
        p.passes_attempted,
        p.pass_completion_pct,
        p.key_passes,
        p.passes_into_final_third,
        p.passes_into_penalty_area,
        p.progressive_passes,

        -- Key passes per 90
        ROUND(p.key_passes / NULLIF(p.nineties, 0), 2) AS key_passes_per90,
        ROUND(p.progressive_passes / NULLIF(p.nineties, 0), 2) AS progressive_passes_per90,

        -- Defense
        p.tackles,
        p.tackles_won,
        p.interceptions,
        p.blocks,
        p.clearances,
        p.defensive_actions,

        -- Defensive per 90
        ROUND(p.tackles / NULLIF(p.nineties, 0), 2) AS tackles_per90,
        ROUND(p.interceptions / NULLIF(p.nineties, 0), 2) AS interceptions_per90,
        ROUND(p.defensive_actions / NULLIF(p.nineties, 0), 2) AS defensive_actions_per90,

        -- Tackle success rate
        CASE
            WHEN p.tackles > 0 THEN ROUND(100.0 * p.tackles_won / p.tackles, 1)
            ELSE NULL
        END AS tackle_success_pct,

        -- Creativity
        p.sca,
        p.sca_per90,
        p.gca,
        p.gca_per90,

        -- Discipline
        p.yellow_cards,
        p.red_cards,
        p.yellow_cards + p.red_cards AS total_cards,
        ROUND((p.yellow_cards + p.red_cards) / NULLIF(p.nineties, 0), 2) AS cards_per90,

        -- Fouls committed estimation (based on cards)
        CASE
            WHEN p.nineties > 0 THEN
                CASE
                    WHEN (p.yellow_cards + p.red_cards) / p.nineties > 0.3 THEN 'High'
                    WHEN (p.yellow_cards + p.red_cards) / p.nineties > 0.15 THEN 'Medium'
                    ELSE 'Low'
                END
            ELSE 'Unknown'
        END AS discipline_risk,

        -- Performance ratings
        CASE
            WHEN p.position_category = 'Forward' THEN
                ROUND(
                    (COALESCE(p.goals_per90, 0) * 30) +
                    (COALESCE(p.assists_per90, 0) * 20) +
                    (COALESCE(p.shot_conversion_pct, 0) * 0.3) +
                    (COALESCE(p.goals_minus_xg, 0) * 5),
                    1
                )
            WHEN p.position_category = 'Midfielder' THEN
                ROUND(
                    (COALESCE(p.goals_per90, 0) * 15) +
                    (COALESCE(p.assists_per90, 0) * 25) +
                    (COALESCE(p.key_passes, 0) / NULLIF(p.nineties, 0) * 5) +
                    (COALESCE(p.pass_completion_pct, 0) * 0.2) +
                    (COALESCE(p.progressive_passes, 0) / NULLIF(p.nineties, 0) * 3),
                    1
                )
            WHEN p.position_category = 'Defender' THEN
                ROUND(
                    (COALESCE(p.tackles, 0) / NULLIF(p.nineties, 0) * 10) +
                    (COALESCE(p.interceptions, 0) / NULLIF(p.nineties, 0) * 10) +
                    (COALESCE(p.clearances, 0) / NULLIF(p.nineties, 0) * 5) +
                    (COALESCE(p.pass_completion_pct, 0) * 0.3),
                    1
                )
            ELSE NULL
        END AS performance_score,

        p.transformed_at

    FROM players p
)

SELECT * FROM final
