{%- macro sql_for_stage2_hubs(src) -%}
{{
    config(
        materialized='incremental',
        unique_key='HashKey'
    )
}}
with source as (
    {% for sat in src %}
    select 
            {{ get_field_mappings_unsafe(this.name) }}
            ,dbt_updated_at

        from {{ ref(sat) }} 
        where dbt_valid_to is null 
        {% if is_incremental() %}
            and dbt_updated_at > (select max(dbt_updated_at) from {{ this }})
        {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {%- endfor -%}
),

finally as (
    select * from source
)

select * from finally

{%- endmacro -%}