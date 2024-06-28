{%- macro sql_for_stage1_models() -%}

with source as (
    select 
            {{ get_field_mappings_unsafe(this.name) }}
            
        from {{ get_source_dataset_definition(this.name) }}
        where {{ get_source_dataset_filter(this.name) }}
),

finally as (
    select * from source
)

select * from finally

{%- endmacro -%}