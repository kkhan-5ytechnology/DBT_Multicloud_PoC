{% macro check_table_exists(tablename) -%}
    {{ return(adapter.dispatch('check_table_exists')(tablename)) }}
{%- endmacro %}

{% macro default__check_table_exists(tablename) %}
    {% set sql_statement %}
        SELECT 
                '[FOUND]' 
            FROM sys.tables 
            WHERE [name] = '{{ tablename }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}

{% macro fabric__check_table_exists(tablename) %}
    {% set sql_statement %}
        SELECT 
                '[FOUND]' 
            FROM sys.tables 
            WHERE [name] = '{{ tablename }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}

{% macro databricks__check_table_exists(tablename) %}
    {% set sql_statement %}
        select 
                '[FOUND]' 
            from INFORMATION_SCHEMA.TABLES 
            where TABLE_NAME = lower('{{ tablename }}')
                and TABLE_TYPE = 'MANAGED'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}
