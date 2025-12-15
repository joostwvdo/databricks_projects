{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Combined Match Results with xG
    Combines Football-Data.org and FBref data for comprehensive match analysis
*/

WITH api_matches AS (
    SELECT
        match_id,
        match_date,
        match_datetime,
        matchday_number,
        competition_code,
        competition_name,
        home_team_id,
        home_team_name,
        away_team_id,
        away_team_name,
        home_goals,
        away_goals,
        total_goals,
        match_result,
        is_draw,
        is_finished,
        'football_data_api' AS primary_source,
        transformed_at
    FROM {{ ref('stg_matches') }}
),

fbref_matches AS (
    SELECT
        match_key,
        match_date,
        competition_code,
        competition_name,
        home_team_name,
        away_team_name,
        home_goals,
        away_goals,
        home_xg,
        away_xg,
        total_xg,
        home_xg_diff,
        away_xg_diff,
        match_result,
        venue,
        attendance,
        referee,
        is_finished
    FROM {{ ref('stg_fbref_matches') }}
),

-- Join op basis van competition, datum en teams
combined AS (
    SELECT
        COALESCE(a.match_id, f.match_key) AS match_id,
        COALESCE(a.match_date, f.match_date) AS match_date,
        a.match_datetime,
        a.matchday_number,

        -- Competition
        COALESCE(a.competition_code, f.competition_code) AS competition_code,
        COALESCE(a.competition_name, f.competition_name) AS competition_name,

        -- Teams
        a.home_team_id,
        COALESCE(a.home_team_name, f.home_team_name) AS home_team_name,
        a.away_team_id,
        COALESCE(a.away_team_name, f.away_team_name) AS away_team_name,

        -- Scores
        COALESCE(a.home_goals, f.home_goals) AS home_goals,
        COALESCE(a.away_goals, f.away_goals) AS away_goals,
        COALESCE(a.total_goals, f.home_goals + f.away_goals) AS total_goals,

        -- xG from FBref
        f.home_xg,
        f.away_xg,
        f.total_xg,
        f.home_xg_diff,
        f.away_xg_diff,

        -- Result
        COALESCE(a.match_result, f.match_result) AS match_result,
        COALESCE(a.is_draw, f.match_result = 'DRAW') AS is_draw,
        COALESCE(a.is_finished, f.is_finished) AS is_finished,

        -- Match details from FBref
        f.venue,
        f.attendance,
        f.referee,

        -- Points
        CASE
            WHEN COALESCE(a.match_result, f.match_result) = 'HOME' THEN 3
            WHEN COALESCE(a.match_result, f.match_result) = 'DRAW' THEN 1
            ELSE 0
        END AS home_points,
        CASE
            WHEN COALESCE(a.match_result, f.match_result) = 'AWAY' THEN 3
            WHEN COALESCE(a.match_result, f.match_result) = 'DRAW' THEN 1
            ELSE 0
        END AS away_points,

        -- Data source flags
        CASE WHEN a.match_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_api_data,
        CASE WHEN f.match_key IS NOT NULL THEN TRUE ELSE FALSE END AS has_fbref_data,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM api_matches a
    FULL OUTER JOIN fbref_matches f
        ON a.competition_code = f.competition_code
        AND a.match_date = f.match_date
        AND (
            LOWER(a.home_team_name) LIKE '%' || LOWER(SUBSTRING(f.home_team_name, 1, 5)) || '%'
            OR LOWER(f.home_team_name) LIKE '%' || LOWER(SUBSTRING(a.home_team_name, 1, 5)) || '%'
        )
)

SELECT * FROM combined
WHERE is_finished = TRUE
