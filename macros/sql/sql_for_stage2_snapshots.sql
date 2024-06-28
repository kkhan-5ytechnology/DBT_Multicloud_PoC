{%- macro sql_for_stage2_snapshots() -%}
{{ config(
    post_hook= ["{{ snapshot_version() }}","{{ snapshot_inserted() }}","{{ snapshot_updated() }}","{{ snapshot_deleted() }}"]
) }}
{%- set src = this.name | replace("SAT_", "SRC_") -%}

    select 
            {{ get_field_mappings_unsafe(this.name) }}

            -- standard SAT data-vault fields
            ,HashKey
            ,HashDiff
            {% if check_table_exists(this.name) == '[FOUND]' %}
            ,0 as dbt_version
            ,'' as dbt_state
            {% else %}
            ,1 as dbt_version
            ,'Initial load - active in vault' as dbt_state
            {% endif %}

        from {{ ref(src) }} 

{%- endmacro -%}