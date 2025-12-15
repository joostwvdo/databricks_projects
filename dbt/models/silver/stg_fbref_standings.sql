{{
    config(
        materialized='incremental',
        unique_key='standing_version_key',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current', 'transformed_at']
    )
}}

/*
    Silver Layer - Staged FBref Standings (SCD Type 2)
    Historische laag voor competitiestanden
    - Bewaart dagelijkse/wekelijkse snapshots van de stand
    - Ideaal voor trend analyses over het seizoen
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'fbref_standings') }}
),

cleaned AS (
    SELECT
        -- Generate unique ID
        {{ dbt_utils.generate_surrogate_key(['competition_name', 'season', 'team_name']) }} AS standing_key,

        -- Competition
        TRIM(competition_name) AS competition_name,
        CASE
            WHEN competition_name = 'Eredivisie' THEN 'DED'
            WHEN competition_name = 'Premier League' THEN 'PL'
            WHEN competition_name = 'La Liga' THEN 'PD'
            WHEN competition_name = 'Serie A' THEN 'SA'
            ELSE NULL
        END AS competition_code,
        TRIM(season) AS season,

        -- Position & Team
        position,
        TRIM(team_name) AS team_name,

        -- Basic stats
        matches_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_difference,
        points,

        -- xG stats (unique to FBref)
        ROUND(xg, 2) AS xg,
        ROUND(xga, 2) AS xga,
        ROUND(COALESCE(xg, 0) - COALESCE(xga, 0), 2) AS xg_difference,

        -- Performance metrics
        ROUND(
            CAST(points AS DOUBLE) / NULLIF(matches_played, 0),
            2
        ) AS points_per_game,

        ROUND(
            CAST(goals_for AS DOUBLE) / NULLIF(matches_played, 0),
            2
        ) AS goals_per_game,

        ROUND(
            CAST(goals_against AS DOUBLE) / NULLIF(matches_played, 0),
            2
        ) AS goals_conceded_per_game,

        -- xG performance (actual vs expected)
        ROUND(goals_for - COALESCE(xg, 0), 2) AS goals_vs_xg,
        ROUND(goals_against - COALESCE(xga, 0), 2) AS goals_conceded_vs_xga,

        -- Win rate
        ROUND(
            100.0 * wins / NULLIF(matches_played, 0),
            1
        ) AS win_percentage,

        -- Metadata
        'fbref' AS data_source,
        CAST(ingest_timestamp AS TIMESTAMP) AS ingested_at,

        -- SCD Type 2 fields
        CAST(ingest_timestamp AS TIMESTAMP) AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,

        -- Hash voor change detection
        MD5(CONCAT(
            COALESCE(CAST(position AS STRING), ''),
            COALESCE(CAST(matches_played AS STRING), ''),
            COALESCE(CAST(points AS STRING), ''),
            COALESCE(CAST(wins AS STRING), ''),
            COALESCE(CAST(goals_for AS STRING), ''),
            COALESCE(CAST(goals_against AS STRING), '')
        )) AS record_hash,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM source
    WHERE team_name IS NOT NULL
      AND TRIM(team_name) != ''
      AND position IS NOT NULL
),

final AS (
    SELECT
        standing_key,
        {{ dbt_utils.generate_surrogate_key(['standing_key', 'valid_from']) }} AS standing_version_key,
        competition_name,
        competition_code,
        season,
        position,
        team_name,
        matches_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_difference,
        points,
        xg,
        xga,
        xg_difference,
        points_per_game,
        goals_per_game,
        goals_conceded_per_game,
        goals_vs_xg,
        goals_conceded_vs_xga,
        win_percentage,
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
    ON f.standing_key = existing.standing_key
    AND existing.is_current = TRUE
WHERE existing.standing_key IS NULL
   OR f.record_hash != existing.record_hash

{% else %}

SELECT * FROM final

{% endif %}
