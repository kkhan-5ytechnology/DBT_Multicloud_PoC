{% macro snapshot_version() -%}
    {{ return(adapter.dispatch('snapshot_version')()) }}
{%- endmacro %}

{% macro default__snapshot_version() %}
    {% set sql_statement %}
        update sat set sat.dbt_version = (1 + (select MAX(dbt_version) from {{ this }} x where x.HashKey = sat.HashKey)) 
            from {{ this }} sat where sat.dbt_version = 0 
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}

{% macro fabric__snapshot_version() %}
    {% set sql_statement %}
        update sat set sat.dbt_version = (1 + (select MAX(dbt_version) from {{ this }} x where x.HashKey = sat.HashKey)) 
            from {{ this }} sat where sat.dbt_version = 0 
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}

{% macro databricks__snapshot_version() %}
    {% set sql_statement %}
        create or replace view ref.snapshot_version as
        select 
            HashKey, 
            (1 + (select max(dbt_version) from {{ this }} x where x.HashKey = sat.HashKey)) as dbt_version
        from {{ this }} sat
        where dbt_version = 0
    {% endset %}
    {% do run_query(sql_statement) %}
    {% set sql_statement %}
        merge into {{ this }} as sat using ref.snapshot_version
            on sat.HashKey = snapshot_version.HashKey
        when matched then
            update set
            dbt_version = snapshot_version.dbt_version
    {% endset %}
    {% do run_query(sql_statement) %}    
{% endmacro %}
