{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override default schema naming to use exactly what we specify
        without prepending the target schema
    #}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
