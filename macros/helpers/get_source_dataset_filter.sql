{% macro get_source_dataset_filter(modelname) -%}
    {{ return(adapter.dispatch('get_source_dataset_filter')(modelname)) }}
{%- endmacro %}

{% macro default__get_source_dataset_filter(modelname) %}
    {% set sql_statement %}
        SELECT WhereClause FROM REF.SourceDatasetDefinitions
            WHERE TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="1 = 1") %}
	{{ return(result) }}
{% endmacro %}

{% macro fabric__get_source_dataset_filter(modelname) %}
    {% set sql_statement %}
        SELECT WhereClause FROM REF.SourceDatasetDefinitions
            WHERE TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="1 = 1") %}
	{{ return(result) }}
{% endmacro %}

{% macro databricks__get_source_dataset_filter(modelname) %}
    {% set sql_statement %}
        select WhereClause from ref.SourceDatasetDefinitions 
            where TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="1 = 1") %}
	{{ return(result) }}
{% endmacro %}
