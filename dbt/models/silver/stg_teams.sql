{{
    config(
        materialized='incremental',
        unique_key='team_version_key',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current', 'transformed_at']
    )
}}

/*
    Silver Layer - Staged Teams (SCD Type 2)
    Historische laag met versiebeheer voor teams
    - Houdt wijzigingen bij in team info (naam, stadion, etc.)
    - Oude versies worden afgesloten, nieuwe toegevoegd
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'teams') }}
),

cleaned AS (
    SELECT
        -- Primary key
        team_id,

        -- Team info
        TRIM(team_name) AS team_name,
        TRIM(short_name) AS short_name,
        UPPER(TRIM(tla)) AS team_code,

        -- Details
        crest_url AS logo_url,
        TRIM(address) AS address,
        TRIM(website) AS website,
        founded AS founded_year,
        TRIM(club_colors) AS club_colors,
        TRIM(venue) AS stadium_name,

        -- Competition
        competition_id,
        TRIM(competition_name) AS competition_name,

        -- Metadata
        CAST(ingest_timestamp AS TIMESTAMP) AS ingested_at,

        -- SCD Type 2 fields
        CAST(ingest_timestamp AS TIMESTAMP) AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,

        -- Hash voor change detection
        MD5(CONCAT(
            COALESCE(TRIM(team_name), ''),
            COALESCE(TRIM(short_name), ''),
            COALESCE(TRIM(venue), ''),
            COALESCE(TRIM(club_colors), ''),
            COALESCE(CAST(competition_id AS STRING), '')
        )) AS record_hash,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM source
    WHERE team_id IS NOT NULL
),

-- Deduplicatie binnen batch: neem meest recente record per team
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY team_id
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM cleaned
),

final AS (
    SELECT
        -- Surrogate key voor versie
        {{ dbt_utils.generate_surrogate_key(['team_id', 'valid_from']) }} AS team_version_key,
        team_id,
        team_name,
        short_name,
        team_code,
        logo_url,
        address,
        website,
        founded_year,
        club_colors,
        stadium_name,
        competition_id,
        competition_name,
        ingested_at,
        valid_from,
        valid_to,
        is_current,
        record_hash,
        transformed_at
    FROM deduplicated
    WHERE row_num = 1
)

{% if is_incremental() %}

-- Bij incremental: alleen nieuwe of gewijzigde records
SELECT f.*
FROM final f
LEFT JOIN {{ this }} existing
    ON f.team_id = existing.team_id
    AND existing.is_current = TRUE
WHERE existing.team_id IS NULL  -- Nieuwe teams
   OR f.record_hash != existing.record_hash  -- Gewijzigde teams

{% else %}

SELECT * FROM final

{% endif %}
