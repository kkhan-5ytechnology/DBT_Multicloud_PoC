{% macro check_view_exists(viewname) -%}
    {{ return(adapter.dispatch('check_view_exists')(viewname)) }}
{%- endmacro %}

{% macro default__check_view_exists(viewname) %}
    {% set sql_statement %}
        SELECT 
                '[FOUND]' 
            FROM sys.views 
            WHERE [name] = '{{ viewname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}

{% macro fabric__check_view_exists(viewname) %}
    {% set sql_statement %}
        SELECT 
                '[FOUND]' 
            FROM sys.views 
            WHERE [name] = '{{ viewname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}

{% macro databricks__check_view_exists(viewname) %}
    {% set sql_statement %}
        select 
                '[FOUND]' 
            from INFORMATION_SCHEMA.TABLES 
            where TABLE_NAME = lower('{{ viewname }}')
                and TABLE_TYPE = 'VIEW'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}
