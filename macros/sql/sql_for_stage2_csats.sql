{%- macro sql_for_stage2_csats() -%}
{{
    config(
        materialized='table'
    )
}}
{%- set src = this.name | replace("CSAT_", "SAT_") -%}

with source as (
    select 
            {{ get_field_mappings_unsafe(this.name) }}

            -- standard CSAT data-vault fields
            ,HashKey as dbt_hashkey
            ,cast(getdate() as datetime2(6)) as dbt_updated_at

        from {{ ref(src) }} 
        where [dbt_valid_to] is null
),

finally as (
    select * from source
)

select * from finally

{%- endmacro -%}