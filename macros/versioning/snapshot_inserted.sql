{% macro snapshot_inserted() -%}
    {{ return(adapter.dispatch('snapshot_inserted')()) }}
{%- endmacro %}

{% macro default__snapshot_inserted() %}
    {% set sql_statement %}
        update {{ this }} 
                set dbt_state = 'Inserted - active in vault' 
            where dbt_state = '' 
                and dbt_valid_to is null 
                and HashKey in (select HashKey from {{ this }} group by HashKey having COUNT(*) = 1) 
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}

{% macro fabric__snapshot_inserted() %}
    {% set sql_statement %}
        update {{ this }} 
                set dbt_state = 'Inserted - active in vault' 
            where dbt_state = '' 
                and dbt_valid_to is null 
                and HashKey in (select HashKey from {{ this }} group by HashKey having COUNT(*) = 1) 
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}

{% macro databricks__inserted() %}
    {% set sql_statement %}
        update {{ this }} 
                set dbt_state = 'Inserted - active in vault' 
            where dbt_state = '' 
                and dbt_valid_to is null 
                and HashKey in (select HashKey from {{ this }} group by HashKey having COUNT(*) = 1) 
    {% endset %}
    {{ return(sql_statement) }}
{% endmacro %}
