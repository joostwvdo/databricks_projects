{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Match Results Fact Table
    Complete match results with team information
*/

WITH matches AS (
    SELECT * FROM {{ ref('stg_matches') }}
    WHERE is_finished = TRUE
),

home_teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
),

away_teams AS (
    SELECT * FROM {{ ref('stg_teams') }}
),

final AS (
    SELECT
        -- Match keys
        m.match_id,
        m.match_date,
        m.match_datetime,
        m.matchday_number,

        -- Competition
        m.competition_code,
        m.competition_name,

        -- Home team
        m.home_team_id,
        m.home_team_name,
        ht.team_code AS home_team_code,
        ht.stadium_name AS home_stadium,

        -- Away team
        m.away_team_id,
        m.away_team_name,
        at.team_code AS away_team_code,

        -- Score
        m.home_goals,
        m.away_goals,
        m.total_goals,
        m.home_goals_halftime,
        m.away_goals_halftime,

        -- Goals in second half
        (m.home_goals - m.home_goals_halftime) AS home_goals_second_half,
        (m.away_goals - m.away_goals_halftime) AS away_goals_second_half,

        -- Result
        m.match_result,
        m.is_draw,

        -- Points calculation
        CASE
            WHEN m.match_result = 'HOME' THEN 3
            WHEN m.match_result = 'DRAW' THEN 1
            ELSE 0
        END AS home_points,

        CASE
            WHEN m.match_result = 'AWAY' THEN 3
            WHEN m.match_result = 'DRAW' THEN 1
            ELSE 0
        END AS away_points,

        -- Goal difference
        (m.home_goals - m.away_goals) AS home_goal_difference,
        (m.away_goals - m.home_goals) AS away_goal_difference,

        -- Match characteristics
        CASE WHEN m.total_goals >= 3 THEN TRUE ELSE FALSE END AS is_high_scoring,
        CASE WHEN m.home_goals = 0 AND m.away_goals = 0 THEN TRUE ELSE FALSE END AS is_goalless,
        CASE WHEN m.home_goals > 0 AND m.away_goals > 0 THEN TRUE ELSE FALSE END AS both_teams_scored,

        -- Metadata
        m.transformed_at

    FROM matches m
    LEFT JOIN home_teams ht ON m.home_team_id = ht.team_id
    LEFT JOIN away_teams at ON m.away_team_id = at.team_id
)

SELECT * FROM final
