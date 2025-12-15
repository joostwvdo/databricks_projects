{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Standing History / Position Trend
    Track hoe teams door het seizoen heen bewegen in de stand
    Gebaseerd op SCD Type 2 historische data
*/

WITH all_standings AS (
    SELECT
        standing_key,
        competition_name,
        competition_code,
        season,
        team_name,
        position,
        matches_played,
        points,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        goal_difference,
        xg,
        xga,
        valid_from,
        valid_to,
        is_current
    FROM {{ ref('stg_fbref_standings') }}
),

-- Bereken positie veranderingen
position_changes AS (
    SELECT
        *,
        LAG(position) OVER (
            PARTITION BY team_name, competition_name, season
            ORDER BY valid_from
        ) AS previous_position,

        LAG(points) OVER (
            PARTITION BY team_name, competition_name, season
            ORDER BY valid_from
        ) AS previous_points,

        LAG(matches_played) OVER (
            PARTITION BY team_name, competition_name, season
            ORDER BY valid_from
        ) AS previous_matches,

        ROW_NUMBER() OVER (
            PARTITION BY team_name, competition_name, season
            ORDER BY valid_from
        ) AS snapshot_number,

        COUNT(*) OVER (
            PARTITION BY team_name, competition_name, season
        ) AS total_snapshots

    FROM all_standings
),

-- Enriched met trends
with_trends AS (
    SELECT
        *,

        -- Position movement
        COALESCE(previous_position, position) - position AS position_change,

        -- Points gained since last snapshot
        points - COALESCE(previous_points, 0) AS points_gained,

        -- Matches played since last snapshot
        matches_played - COALESCE(previous_matches, 0) AS matches_since_last,

        -- Points per game in this period
        CASE
            WHEN matches_played - COALESCE(previous_matches, 0) > 0 THEN
                ROUND(
                    (points - COALESCE(previous_points, 0)) /
                    (matches_played - COALESCE(previous_matches, 0)),
                    2
                )
            ELSE NULL
        END AS period_ppg

    FROM position_changes
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['team_name', 'competition_name', 'season', 'valid_from']) }} AS history_key,

    team_name,
    competition_name,
    competition_code,
    season,

    -- Current snapshot info
    snapshot_number,
    total_snapshots,
    valid_from AS snapshot_date,
    is_current,

    -- Position
    position,
    previous_position,
    position_change,

    -- Points
    points,
    previous_points,
    points_gained,

    -- Match context
    matches_played,
    matches_since_last,
    period_ppg,

    -- Full stats
    wins,
    draws,
    losses,
    goals_for,
    goals_against,
    goal_difference,

    -- xG
    xg,
    xga,
    ROUND(xg - xga, 2) AS xg_difference,

    -- Movement indicators
    CASE
        WHEN position_change > 0 THEN '📈 Up ' || CAST(position_change AS STRING)
        WHEN position_change < 0 THEN '📉 Down ' || CAST(ABS(position_change) AS STRING)
        ELSE '➡️ Same'
    END AS position_movement,

    -- Trend over last snapshots
    CASE
        WHEN position <= 4 THEN 'Champions League Zone'
        WHEN position <= 6 THEN 'Europa League Zone'
        WHEN position <= 7 THEN 'Conference League Zone'
        WHEN position >= 18 THEN 'Relegation Zone'
        WHEN position >= 15 THEN 'Relegation Battle'
        ELSE 'Mid-table'
    END AS table_zone,

    CURRENT_TIMESTAMP() AS transformed_at

FROM with_trends
ORDER BY competition_name, season, team_name, valid_from
