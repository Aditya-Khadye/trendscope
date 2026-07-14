{#
  Use custom schema names verbatim (staging, intermediate, marts, ...)
  instead of dbt's default "<target_schema>_<custom>" concatenation,
  which would produce main_staging etc. in DuckDB.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
