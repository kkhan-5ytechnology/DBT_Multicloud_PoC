{% macro snapshot_updated() %}
    {% set sql_statement %}
        update {{ this }} set dbt_state = 'Updated - versioned in vault' 
            where dbt_state = '' and dbt_valid_to is null and HashKey in (select HashKey from {{ this }} group by HashKey having COUNT(*) > 1) 
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}