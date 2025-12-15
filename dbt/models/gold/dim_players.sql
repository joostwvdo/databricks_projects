{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Player Dimension
    Complete player profiles with aggregated career stats
*/

WITH players AS (
    SELECT * FROM {{ ref('stg_players') }}
),

-- Aggregate across all seasons/competitions for each player
player_summary AS (
    SELECT
        player_name,
        MAX(nationality) AS nationality,
        MAX(birth_year) AS birth_year,

        -- Current team (most recent)
        FIRST_VALUE(team_name) OVER (
            PARTITION BY player_name
            ORDER BY season DESC, minutes DESC
        ) AS current_team,

        -- Primary position
        MODE(position_category) AS primary_position,

        -- Aggregate stats
        COUNT(DISTINCT competition_name) AS competitions_played,
        SUM(matches_played) AS total_matches,
        SUM(minutes) AS total_minutes,
        SUM(goals) AS total_goals,
        SUM(assists) AS total_assists,
        SUM(goal_contributions) AS total_goal_contributions,
        SUM(xg) AS total_xg,
        SUM(xa) AS total_xa,
        SUM(yellow_cards) AS total_yellow_cards,
        SUM(red_cards) AS total_red_cards

    FROM players
    GROUP BY player_name
),

final AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['p.player_name']) }} AS player_key,

        -- Player info
        p.player_name,
        p.nationality,
        p.birth_year,
        2025 - p.birth_year AS age,

        -- Current season details
        p.team_name AS current_team,
        p.competition_name AS current_competition,
        p.position,
        p.position_category,
        p.season AS current_season,

        -- Current season stats
        p.matches_played,
        p.starts,
        p.minutes,
        p.nineties,

        -- Goals & Assists
        p.goals,
        p.assists,
        p.goal_contributions,
        p.non_penalty_goals,
        p.penalty_goals,

        -- xG Performance
        p.xg,
        p.npxg,
        p.xa,
        p.xg_xa,
        p.goals_minus_xg,
        p.assists_minus_xa,

        -- Per 90 metrics
        p.goals_per90,
        p.assists_per90,
        p.xg_per90,
        p.xa_per90,

        -- Shooting
        p.shots,
        p.shots_on_target,
        p.shot_accuracy_pct,
        p.shot_conversion_pct,

        -- Passing
        p.passes_completed,
        p.pass_completion_pct,
        p.key_passes,
        p.progressive_passes,

        -- Defense
        p.tackles,
        p.interceptions,
        p.defensive_actions,

        -- Creativity
        p.sca,
        p.sca_per90,
        p.gca,
        p.gca_per90,

        -- Discipline
        p.yellow_cards,
        p.red_cards,

        -- Player classification
        CASE
            WHEN p.position_category = 'Forward' AND p.goals_per90 >= 0.5 THEN 'Elite Striker'
            WHEN p.position_category = 'Forward' AND p.goals_per90 >= 0.3 THEN 'Goal Scorer'
            WHEN p.position_category = 'Forward' THEN 'Forward'
            WHEN p.position_category = 'Midfielder' AND p.assists_per90 >= 0.3 THEN 'Playmaker'
            WHEN p.position_category = 'Midfielder' AND p.goals_per90 >= 0.2 THEN 'Box-to-Box'
            WHEN p.position_category = 'Midfielder' THEN 'Midfielder'
            WHEN p.position_category = 'Defender' AND p.tackles + p.interceptions >= 5 THEN 'Ball Winner'
            WHEN p.position_category = 'Defender' THEN 'Defender'
            WHEN p.position_category = 'Goalkeeper' THEN 'Goalkeeper'
            ELSE 'Unknown'
        END AS player_type,

        -- Finishing quality
        CASE
            WHEN p.minutes >= 450 AND p.goals_minus_xg >= 3 THEN 'Elite Finisher'
            WHEN p.minutes >= 450 AND p.goals_minus_xg > 0 THEN 'Good Finisher'
            WHEN p.minutes >= 450 AND p.goals_minus_xg < -3 THEN 'Poor Finisher'
            WHEN p.minutes >= 450 AND p.goals_minus_xg <= 0 THEN 'Below Average Finisher'
            ELSE 'Insufficient Data'
        END AS finishing_quality,

        p.transformed_at

    FROM players p
)

SELECT * FROM final
