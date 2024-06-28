{%- macro sql_for_stage3_dims(src) -%}

with source as (
    {% for sat in src %}
    select 
            {{ get_field_mappings_safe(this.name, sat) }}
            ,a.dbt_updated_at

        from {{ ref(sat) }} a 
        {%- if check_table_exists('C' + sat) == '[FOUND]' %} inner join {{ ref(sat) | replace("SAT_", "CSAT_") }} b on a.HashKey = b.dbt_hashkey and a.dbt_valid_to is null 
        {% else %}
        where dbt_valid_to is null 
        {%- endif -%}

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