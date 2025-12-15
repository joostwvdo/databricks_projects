{{
    config(
        materialized='incremental',
        unique_key='player_version_key',
        incremental_strategy='merge',
        merge_update_columns=['valid_to', 'is_current', 'transformed_at']
    )
}}

/*
    Silver Layer - Staged Player Statistics (SCD Type 2)
    Historische laag met versiebeheer voor spelers
    - Houdt wijzigingen bij in stats (goals, assists, etc.)
    - Per ingest worden nieuwe versies aangemaakt bij wijzigingen
    - Oude versies blijven beschikbaar voor historische analyse
*/

WITH source AS (
    SELECT * FROM {{ source('bronze', 'player_stats') }}
),

cleaned AS (
    SELECT
        -- Player info
        TRIM(player_name) AS player_name,
        TRIM(team_name) AS team_name,
        TRIM(nationality) AS nationality,
        TRIM(position) AS position,

        -- Position category
        CASE
            WHEN UPPER(position) LIKE '%GK%' THEN 'Goalkeeper'
            WHEN UPPER(position) LIKE '%DF%' OR UPPER(position) LIKE '%CB%' OR UPPER(position) LIKE '%FB%' OR UPPER(position) LIKE '%WB%' THEN 'Defender'
            WHEN UPPER(position) LIKE '%MF%' OR UPPER(position) LIKE '%DM%' OR UPPER(position) LIKE '%CM%' OR UPPER(position) LIKE '%AM%' THEN 'Midfielder'
            WHEN UPPER(position) LIKE '%FW%' OR UPPER(position) LIKE '%ST%' OR UPPER(position) LIKE '%CF%' OR UPPER(position) LIKE '%LW%' OR UPPER(position) LIKE '%RW%' THEN 'Forward'
            ELSE 'Unknown'
        END AS position_category,

        -- Age & birth
        age,
        birth_year,
        CASE
            WHEN birth_year IS NOT NULL THEN 2025 - birth_year
            ELSE NULL
        END AS calculated_age,

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

        -- Playing time
        COALESCE(matches_played, 0) AS matches_played,
        COALESCE(starts, 0) AS starts,
        COALESCE(minutes, 0) AS minutes,
        COALESCE(nineties, ROUND(COALESCE(minutes, 0) / 90.0, 2)) AS nineties,

        -- Substitute appearances
        COALESCE(matches_played, 0) - COALESCE(starts, 0) AS sub_appearances,

        -- Goals & Assists
        COALESCE(goals, 0) AS goals,
        COALESCE(assists, 0) AS assists,
        COALESCE(goals, 0) + COALESCE(assists, 0) AS goal_contributions,
        COALESCE(non_penalty_goals, 0) AS non_penalty_goals,
        COALESCE(penalty_goals, 0) AS penalty_goals,
        COALESCE(penalty_attempts, 0) AS penalty_attempts,

        -- Cards
        COALESCE(yellow_cards, 0) AS yellow_cards,
        COALESCE(red_cards, 0) AS red_cards,

        -- xG Stats
        ROUND(COALESCE(xg, 0), 2) AS xg,
        ROUND(COALESCE(npxg, 0), 2) AS npxg,
        ROUND(COALESCE(xa, 0), 2) AS xa,
        ROUND(COALESCE(xg, 0) + COALESCE(xa, 0), 2) AS xg_xa,

        -- Performance vs Expected
        ROUND(COALESCE(goals, 0) - COALESCE(xg, 0), 2) AS goals_minus_xg,
        ROUND(COALESCE(assists, 0) - COALESCE(xa, 0), 2) AS assists_minus_xa,

        -- Per 90 stats
        ROUND(goals_per90, 2) AS goals_per90,
        ROUND(assists_per90, 2) AS assists_per90,
        ROUND(xg_per90, 2) AS xg_per90,
        ROUND(xa_per90, 2) AS xa_per90,

        -- Shooting
        COALESCE(shots, 0) AS shots,
        COALESCE(shots_on_target, 0) AS shots_on_target,
        ROUND(shot_accuracy, 1) AS shot_accuracy_pct,
        ROUND(shots_per90, 2) AS shots_per90,

        -- Conversion rate
        CASE
            WHEN COALESCE(shots, 0) > 0 THEN ROUND(100.0 * COALESCE(goals, 0) / shots, 1)
            ELSE NULL
        END AS shot_conversion_pct,

        -- Passing
        COALESCE(passes_completed, 0) AS passes_completed,
        COALESCE(passes_attempted, 0) AS passes_attempted,
        ROUND(pass_completion_pct, 1) AS pass_completion_pct,
        COALESCE(key_passes, 0) AS key_passes,
        COALESCE(passes_into_final_third, 0) AS passes_into_final_third,
        COALESCE(passes_into_penalty_area, 0) AS passes_into_penalty_area,
        COALESCE(progressive_passes, 0) AS progressive_passes,

        -- Defense
        COALESCE(tackles, 0) AS tackles,
        COALESCE(tackles_won, 0) AS tackles_won,
        COALESCE(interceptions, 0) AS interceptions,
        COALESCE(blocks, 0) AS blocks,
        COALESCE(clearances, 0) AS clearances,
        COALESCE(tackles, 0) + COALESCE(interceptions, 0) AS defensive_actions,

        -- GCA (Goal & Shot Creating Actions)
        COALESCE(sca, 0) AS sca,
        ROUND(sca_per90, 2) AS sca_per90,
        COALESCE(gca, 0) AS gca,
        ROUND(gca_per90, 2) AS gca_per90,

        -- Metadata
        'fbref' AS data_source,
        CAST(ingest_timestamp AS TIMESTAMP) AS ingested_at,

        -- SCD Type 2 fields
        CAST(ingest_timestamp AS TIMESTAMP) AS valid_from,
        CAST(NULL AS TIMESTAMP) AS valid_to,
        TRUE AS is_current,

        -- Hash voor change detection (key stats die vaak wijzigen)
        MD5(CONCAT(
            COALESCE(CAST(matches_played AS STRING), ''),
            COALESCE(CAST(minutes AS STRING), ''),
            COALESCE(CAST(goals AS STRING), ''),
            COALESCE(CAST(assists AS STRING), ''),
            COALESCE(CAST(xg AS STRING), ''),
            COALESCE(CAST(xa AS STRING), ''),
            COALESCE(CAST(yellow_cards AS STRING), ''),
            COALESCE(CAST(red_cards AS STRING), ''),
            COALESCE(TRIM(team_name), '')
        )) AS record_hash,

        CURRENT_TIMESTAMP() AS transformed_at

    FROM source
    WHERE player_name IS NOT NULL
      AND TRIM(player_name) != ''
      AND TRIM(player_name) != 'nan'
),

-- Deduplicatie binnen batch
deduplicated AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY player_name, team_name, competition_name, season
            ORDER BY ingested_at DESC
        ) AS row_num
    FROM cleaned
),

final AS (
    SELECT
        -- Business key
        {{ dbt_utils.generate_surrogate_key(['player_name', 'team_name', 'competition_name', 'season']) }} AS player_season_key,
        -- Version key voor SCD
        {{ dbt_utils.generate_surrogate_key(['player_name', 'team_name', 'competition_name', 'season', 'valid_from']) }} AS player_version_key,

        player_name,
        team_name,
        nationality,
        position,
        position_category,
        age,
        birth_year,
        calculated_age,
        competition_name,
        competition_code,
        season,
        matches_played,
        starts,
        minutes,
        nineties,
        sub_appearances,
        goals,
        assists,
        goal_contributions,
        non_penalty_goals,
        penalty_goals,
        penalty_attempts,
        yellow_cards,
        red_cards,
        xg,
        npxg,
        xa,
        xg_xa,
        goals_minus_xg,
        assists_minus_xa,
        goals_per90,
        assists_per90,
        xg_per90,
        xa_per90,
        shots,
        shots_on_target,
        shot_accuracy_pct,
        shots_per90,
        shot_conversion_pct,
        passes_completed,
        passes_attempted,
        pass_completion_pct,
        key_passes,
        passes_into_final_third,
        passes_into_penalty_area,
        progressive_passes,
        tackles,
        tackles_won,
        interceptions,
        blocks,
        clearances,
        defensive_actions,
        sca,
        sca_per90,
        gca,
        gca_per90,
        data_source,
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
    ON f.player_season_key = existing.player_season_key
    AND existing.is_current = TRUE
WHERE existing.player_season_key IS NULL  -- Nieuwe spelers
   OR f.record_hash != existing.record_hash  -- Gewijzigde stats

{% else %}

SELECT * FROM final

{% endif %}
