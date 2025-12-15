{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Head to Head Analysis
    Historische onderlinge resultaten tussen teams
*/

WITH matches AS (
    SELECT
        match_id,
        match_date,
        competition_name,
        competition_code,
        home_team_name,
        away_team_name,
        home_goals,
        away_goals,
        match_result,
        home_xg,
        away_xg,
        is_finished
    FROM {{ ref('stg_fbref_matches') }}
    WHERE is_current = TRUE
      AND is_finished = TRUE
),

-- Creëer alle team combinaties (beide richtingen)
all_matchups AS (
    -- Home perspective
    SELECT
        home_team_name AS team_1,
        away_team_name AS team_2,
        competition_name,
        competition_code,
        match_date,
        home_goals AS team_1_goals,
        away_goals AS team_2_goals,
        home_xg AS team_1_xg,
        away_xg AS team_2_xg,
        CASE
            WHEN match_result = 'HOME' THEN 'WIN'
            WHEN match_result = 'AWAY' THEN 'LOSS'
            ELSE 'DRAW'
        END AS team_1_result,
        'HOME' AS team_1_venue
    FROM matches

    UNION ALL

    -- Away perspective
    SELECT
        away_team_name AS team_1,
        home_team_name AS team_2,
        competition_name,
        competition_code,
        match_date,
        away_goals AS team_1_goals,
        home_goals AS team_2_goals,
        away_xg AS team_1_xg,
        home_xg AS team_2_xg,
        CASE
            WHEN match_result = 'AWAY' THEN 'WIN'
            WHEN match_result = 'HOME' THEN 'LOSS'
            ELSE 'DRAW'
        END AS team_1_result,
        'AWAY' AS team_1_venue
    FROM matches
),

-- Aggregeer per team combinatie
h2h_stats AS (
    SELECT
        -- Sorteer team namen alfabetisch voor consistente keys
        CASE WHEN team_1 < team_2 THEN team_1 ELSE team_2 END AS team_a,
        CASE WHEN team_1 < team_2 THEN team_2 ELSE team_1 END AS team_b,
        team_1,
        team_2,
        competition_name,

        COUNT(*) AS total_matches,
        MIN(match_date) AS first_meeting,
        MAX(match_date) AS last_meeting,

        -- Team 1 stats
        SUM(CASE WHEN team_1_result = 'WIN' THEN 1 ELSE 0 END) AS team_1_wins,
        SUM(CASE WHEN team_1_result = 'DRAW' THEN 1 ELSE 0 END) AS draws,
        SUM(CASE WHEN team_1_result = 'LOSS' THEN 1 ELSE 0 END) AS team_1_losses,

        SUM(team_1_goals) AS team_1_goals_total,
        SUM(team_2_goals) AS team_2_goals_total,

        ROUND(AVG(team_1_goals), 2) AS team_1_avg_goals,
        ROUND(AVG(team_2_goals), 2) AS team_2_avg_goals,

        -- xG stats
        ROUND(SUM(team_1_xg), 2) AS team_1_xg_total,
        ROUND(SUM(team_2_xg), 2) AS team_2_xg_total,

        -- Venue breakdown for team_1
        SUM(CASE WHEN team_1_venue = 'HOME' AND team_1_result = 'WIN' THEN 1 ELSE 0 END) AS team_1_home_wins,
        SUM(CASE WHEN team_1_venue = 'AWAY' AND team_1_result = 'WIN' THEN 1 ELSE 0 END) AS team_1_away_wins

    FROM all_matchups
    GROUP BY
        CASE WHEN team_1 < team_2 THEN team_1 ELSE team_2 END,
        CASE WHEN team_1 < team_2 THEN team_2 ELSE team_1 END,
        team_1,
        team_2,
        competition_name
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_a', 'team_b', 'competition_name']) }} AS h2h_key,

    team_a,
    team_b,
    team_1 AS perspective_team,
    team_2 AS opponent_team,
    competition_name,

    total_matches,
    first_meeting,
    last_meeting,

    -- Record from team_1 perspective
    team_1_wins AS wins,
    draws,
    team_1_losses AS losses,

    -- Points if it were a mini-league
    (team_1_wins * 3) + draws AS points,
    (team_1_losses * 3) + draws AS opponent_points,

    -- Goals
    team_1_goals_total AS goals_scored,
    team_2_goals_total AS goals_conceded,
    team_1_goals_total - team_2_goals_total AS goal_difference,

    team_1_avg_goals AS avg_goals_scored,
    team_2_avg_goals AS avg_goals_conceded,

    -- xG
    team_1_xg_total AS xg_total,
    team_2_xg_total AS xga_total,
    ROUND(team_1_xg_total - team_2_xg_total, 2) AS xg_difference,

    -- Win percentages
    ROUND(100.0 * team_1_wins / total_matches, 1) AS win_percentage,
    ROUND(100.0 * draws / total_matches, 1) AS draw_percentage,
    ROUND(100.0 * team_1_losses / total_matches, 1) AS loss_percentage,

    -- Home/Away breakdown
    team_1_home_wins,
    team_1_away_wins,

    -- Dominance indicator
    CASE
        WHEN team_1_wins > team_1_losses + 2 THEN 'Dominant'
        WHEN team_1_wins > team_1_losses THEN 'Slight Edge'
        WHEN team_1_wins = team_1_losses THEN 'Even'
        WHEN team_1_wins < team_1_losses - 2 THEN 'Inferior'
        ELSE 'Slight Disadvantage'
    END AS h2h_dominance,

    CURRENT_TIMESTAMP() AS transformed_at

FROM h2h_stats
WHERE total_matches >= 1
ORDER BY team_a, team_b, competition_name
