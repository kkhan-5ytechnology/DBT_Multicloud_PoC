{% macro snapshot_deleted() -%}
    {{ return(adapter.dispatch('snapshot_deleted')()) }}
{%- endmacro %}

{% macro default__snapshot_deleted() %}
    {% set sql_statement %}
        with tb1 as (
            select 
                    HashKey, MAX(dbt_version) as dbt_version 
                from {{ this }}	
                where dbt_valid_to is not null 
                    and HashKey not in (select HashKey from {{ this }} where dbt_valid_to is null) group by HashKey
        ) 
        update tb2 
                set tb2.dbt_state = 'Deleted - expired in vault' 
            from {{ this }} tb2 inner join tb1 on tb2.HashKey = tb1.HashKey and tb2.dbt_version = tb1.dbt_version
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}

{% macro fabric__snapshot_deleted() %}
    {% set sql_statement %}
        with tb1 as (
            select 
                    HashKey, MAX(dbt_version) as dbt_version 
                from {{ this }}	
                where dbt_valid_to is not null 
                    and HashKey not in (select HashKey from {{ this }} where dbt_valid_to is null) group by HashKey
        ) 
        update tb2 
                set tb2.dbt_state = 'Deleted - expired in vault' 
            from {{ this }} tb2 inner join tb1 on tb2.HashKey = tb1.HashKey and tb2.dbt_version = tb1.dbt_version
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}

{% macro databricks__snapshot_deleted() %}
    {% set sql_statement %}
        create or replace view ref.snapshot_deleted as
        select 
            HashKey,
            max(dbt_version) as dbt_version
        from {{ this }} sat
        where dbt_valid_to is not null
            and HashKey not in (select HashKey from {{ this }} where dbt_valid_to is null) group by HashKey
    {% endset %}
    {% do run_query(sql_statement) %}
    {% set sql_statement %}
        merge into {{ this }} as sat using ref.snapshot_deleted
            on sat.HashKey = snapshot_deleted.HashKey and sat.dbt_version = snapshot_deleted.dbt_version
        when matched then
            update set
            dbt_state = 'Deleted - expired in vault'
    {% endset %}
    {% do run_query(sql_statement) %} 
{% endmacro %}
