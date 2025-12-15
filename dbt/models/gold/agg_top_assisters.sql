{{
    config(
        materialized='table'
    )
}}

/*
    Gold Layer - Top Assisters
    Ranking of top assist providers per competition
*/

WITH players AS (
    SELECT * FROM {{ ref('fct_player_performance') }}
),

ranked AS (
    SELECT
        -- Ranking
        ROW_NUMBER() OVER (
            PARTITION BY competition_name
            ORDER BY assists DESC, key_passes DESC, minutes ASC
        ) AS rank_in_competition,

        ROW_NUMBER() OVER (
            ORDER BY assists DESC, key_passes DESC, minutes ASC
        ) AS overall_rank,

        -- Player info
        player_name,
        team_name,
        nationality,
        position,
        position_category,
        age,

        -- Competition
        competition_name,
        competition_code,
        season,

        -- Playing time
        matches_played,
        starts,
        minutes,
        nineties,

        -- Assists
        assists,
        assists_per90,
        xa,
        assists_minus_xa,

        -- Creativity metrics
        key_passes,
        key_passes_per90,
        passes_into_final_third,
        passes_into_penalty_area,
        progressive_passes,
        progressive_passes_per90,

        -- SCA/GCA
        sca,
        sca_per90,
        gca,
        gca_per90,

        -- Goals (secondary)
        goals,
        goal_contributions,

        -- Minutes per assist
        CASE
            WHEN assists > 0 THEN ROUND(minutes / assists, 0)
            ELSE NULL
        END AS minutes_per_assist,

        -- Chance creation rate
        CASE
            WHEN nineties > 0 THEN ROUND((key_passes + passes_into_penalty_area) / nineties, 2)
            ELSE NULL
        END AS chance_creation_per90,

        transformed_at

    FROM players
    WHERE assists > 0 OR key_passes >= 10
)

SELECT
    *,

    -- Playmaker rating
    CASE
        WHEN rank_in_competition = 1 THEN '🎯 Top Playmaker'
        WHEN rank_in_competition <= 3 THEN 'Elite Creator'
        WHEN rank_in_competition <= 10 THEN 'Key Player'
        ELSE 'Contributor'
    END AS playmaker_status

FROM ranked
ORDER BY competition_name, rank_in_competition
