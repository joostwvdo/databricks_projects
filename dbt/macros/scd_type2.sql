{% macro close_expired_records(target_table, key_column, hash_column) %}
/*
    Macro om oude SCD Type 2 records af te sluiten
    Wordt aangeroepen na een incremental load om oude versies te updaten
*/

UPDATE {{ target_table }} target
SET
    valid_to = source.valid_from,
    is_current = FALSE,
    transformed_at = CURRENT_TIMESTAMP()
FROM (
    SELECT
        {{ key_column }},
        valid_from,
        {{ hash_column }}
    FROM {{ target_table }}
    WHERE is_current = TRUE
) source
WHERE target.{{ key_column }} = source.{{ key_column }}
  AND target.is_current = TRUE
  AND target.valid_from < source.valid_from
  AND target.{{ hash_column }} != source.{{ hash_column }}

{% endmacro %}
