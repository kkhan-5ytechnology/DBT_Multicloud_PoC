{% macro snapshot_deleted() %}
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