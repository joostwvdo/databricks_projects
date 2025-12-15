{{
    config(
        materialized='incremental',
        unique_key='match_version_key',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current', 'transformed_at']
    )
}}

/*
    Silver Layer - Staged FBref Matches (SCD Type 2)
    Historische laag voor FBref wedstrijddata met xG
    - Houdt score updates bij (live wedstrijden -> eindstand)
    - Bewaart historie van xG wijzigingen
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'fbref_matches') }}
),

cleaned AS (
    SELECT
        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key(['competition_name', 'match_date', 'home_team_name', 'away_team_name']) }} AS match_key,

        -- Match timing
        TRIM(match_date) AS match_date_str,
        TRY_CAST(match_date AS DATE) AS match_date,
        TRIM(match_time) AS match_time,
        TRIM(matchweek) AS matchweek,
        TRIM(season) AS season,

        -- Competition
        TRIM(competition_name) AS competition_name,
        CASE
            WHEN competition_name = 'Eredivisie' THEN 'DED'
            WHEN competition_name = 'Premier League' THEN 'PL'
            WHEN competition_name = 'La Liga' THEN 'PD'
            WHEN competition_name = 'Serie A' THEN 'SA'
            ELSE NULL
        END AS competition_code,

        -- Teams
        TRIM(home_team_name) AS home_team_name,
        TRIM(away_team_name) AS away_team_name,

        -- Scores
        TRY_CAST(home_score AS INT) AS home_goals,
        TRY_CAST(away_score AS INT) AS away_goals,

        -- xG (Expected Goals) - unique to FBref
        ROUND(home_xg, 2) AS home_xg,
        ROUND(away_xg, 2) AS away_xg,
        ROUND(COALESCE(home_xg, 0) + COALESCE(away_xg, 0), 2) AS total_xg,

        -- Match info
        TRIM(venue) AS venue,
        TRIM(referee) AS referee,
        TRIM(attendance) AS attendance_str,
        TRY_CAST(REGEXP_REPLACE(attendance, '[^0-9]', '') AS INT) AS attendance,

        -- Calculated fields
        CASE
            WHEN TRY_CAST(home_score AS INT) > TRY_CAST(away_score AS INT) THEN 'HOME'
            WHEN TRY_CAST(home_score AS INT) < TRY_CAST(away_score AS INT) THEN 'AWAY'
            WHEN TRY_CAST(home_score AS INT) = TRY_CAST(away_score AS INT)
                 AND home_score IS NOT NULL THEN 'DRAW'
            ELSE NULL
        END AS match_result,

        -- xG performance (actual vs expected)
        ROUND(TRY_CAST(home_score AS INT) - COALESCE(home_xg, 0), 2) AS home_xg_diff,
        ROUND(TRY_CAST(away_score AS INT) - COALESCE(away_xg, 0), 2) AS away_xg_diff,

        -- Flags
        CASE WHEN home_score IS NOT NULL AND away_score IS NOT NULL THEN TRUE ELSE FALSE END AS is_finished,

        -- Metadata
        'fbref' AS data_source,
        CAST(ingest_timestamp AS TIMESTAMP) AS ingested_at,

        -- SCD Type 2 fields
        CAST(ingest_timestamp AS TIMESTAMP) AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,

        -- Hash voor change detection
        MD5(CONCAT(
            COALESCE(home_score, ''),
            COALESCE(away_score, ''),
            COALESCE(CAST(home_xg AS STRING), ''),
            COALESCE(CAST(away_xg AS STRING), ''),
            COALESCE(attendance, '')
        )) AS record_hash,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM source
    WHERE home_team_name IS NOT NULL
      AND away_team_name IS NOT NULL
      AND TRIM(home_team_name) != ''
      AND TRIM(away_team_name) != ''
),

final AS (
    SELECT
        match_key,
        {{ dbt_utils.generate_surrogate_key(['match_key', 'valid_from']) }} AS match_version_key,
        match_date_str,
        match_date,
        match_time,
        matchweek,
        season,
        competition_name,
        competition_code,
        home_team_name,
        away_team_name,
        home_goals,
        away_goals,
        home_xg,
        away_xg,
        total_xg,
        venue,
        referee,
        attendance_str,
        attendance,
        match_result,
        home_xg_diff,
        away_xg_diff,
        is_finished,
        data_source,
        ingested_at,
        valid_from,
        valid_to,
        is_current,
        record_hash,
        transformed_at
    FROM cleaned
)

{% if is_incremental() %}

SELECT f.*
FROM final f
LEFT JOIN {{ this }} existing
    ON f.match_key = existing.match_key
    AND existing.is_current = TRUE
WHERE existing.match_key IS NULL
   OR f.record_hash != existing.record_hash

{% else %}

SELECT * FROM final

{% endif %}
