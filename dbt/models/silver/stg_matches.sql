{{
    config(
        materialized='incremental',
        unique_key='match_id',
        incremental_strategy='merge',
        merge_update_columns=['match_status', 'home_goals', 'away_goals', 'home_goals_halftime', 'away_goals_halftime', 'match_result', 'is_finished', 'is_draw', 'source_updated_at', 'valid_to', 'is_current', 'transformed_at']
    )
}}

/*
    Silver Layer - Staged Matches (SCD Type 2)
    Historische laag met versiebeheer voor wedstrijden
    - Nieuwe records krijgen is_current = TRUE
    - Bij updates wordt oude record afgesloten (is_current = FALSE, valid_to = now)
    - Nieuw record wordt toegevoegd met is_current = TRUE
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'matches') }}
),

cleaned AS (
    SELECT
        -- Primary key
        match_id,

        -- Match timing
        CAST(utc_date AS TIMESTAMP) AS match_datetime,
        DATE(utc_date) AS match_date,
        EXTRACT(YEAR FROM utc_date) AS match_year,
        EXTRACT(MONTH FROM utc_date) AS match_month,
        DAYOFWEEK(utc_date) AS day_of_week,

        -- Match info
        UPPER(TRIM(status)) AS match_status,
        matchday AS matchday_number,
        UPPER(TRIM(stage)) AS stage,

        -- Competition
        competition_id,
        TRIM(competition_name) AS competition_name,
        UPPER(TRIM(competition_code)) AS competition_code,

        -- Home team
        home_team_id,
        TRIM(home_team_name) AS home_team_name,
        UPPER(TRIM(home_team_short_name)) AS home_team_short_name,

        -- Away team
        away_team_id,
        TRIM(away_team_name) AS away_team_name,
        UPPER(TRIM(away_team_short_name)) AS away_team_short_name,

        -- Scores
        COALESCE(home_score_fulltime, 0) AS home_goals,
        COALESCE(away_score_fulltime, 0) AS away_goals,
        COALESCE(home_score_halftime, 0) AS home_goals_halftime,
        COALESCE(away_score_halftime, 0) AS away_goals_halftime,

        -- Calculated fields
        COALESCE(home_score_fulltime, 0) + COALESCE(away_score_fulltime, 0) AS total_goals,

        CASE
            WHEN winner = 'HOME_TEAM' THEN 'HOME'
            WHEN winner = 'AWAY_TEAM' THEN 'AWAY'
            WHEN winner = 'DRAW' THEN 'DRAW'
            ELSE NULL
        END AS match_result,

        -- Boolean flags
        CASE WHEN status = 'FINISHED' THEN TRUE ELSE FALSE END AS is_finished,
        CASE WHEN winner = 'DRAW' THEN TRUE ELSE FALSE END AS is_draw,

        -- Metadata
        CAST(last_updated AS TIMESTAMP) AS source_updated_at,
        CAST(ingest_timestamp AS TIMESTAMP) AS ingested_at,

        -- SCD Type 2 fields
        CAST(ingest_timestamp AS TIMESTAMP) AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,

        -- Hash voor change detection
        MD5(CONCAT(
            COALESCE(CAST(status AS STRING), ''),
            COALESCE(CAST(home_score_fulltime AS STRING), ''),
            COALESCE(CAST(away_score_fulltime AS STRING), ''),
            COALESCE(winner, '')
        )) AS record_hash,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM source
    WHERE match_id IS NOT NULL
)

{% if is_incremental() %}

-- Bij incremental: check voor wijzigingen
SELECT
    c.match_id,
    c.match_datetime,
    c.match_date,
    c.match_year,
    c.match_month,
    c.day_of_week,
    c.match_status,
    c.matchday_number,
    c.stage,
    c.competition_id,
    c.competition_name,
    c.competition_code,
    c.home_team_id,
    c.home_team_name,
    c.home_team_short_name,
    c.away_team_id,
    c.away_team_name,
    c.away_team_short_name,
    c.home_goals,
    c.away_goals,
    c.home_goals_halftime,
    c.away_goals_halftime,
    c.total_goals,
    c.match_result,
    c.is_finished,
    c.is_draw,
    c.source_updated_at,
    c.ingested_at,
    c.valid_from,
    c.valid_to,
    c.is_current,
    c.record_hash,
    c.transformed_at
FROM cleaned c
LEFT JOIN {{ this }} existing
    ON c.match_id = existing.match_id
    AND existing.is_current = TRUE
WHERE existing.match_id IS NULL  -- Nieuwe records
   OR c.record_hash != existing.record_hash  -- Gewijzigde records

{% else %}

SELECT * FROM cleaned

{% endif %}
