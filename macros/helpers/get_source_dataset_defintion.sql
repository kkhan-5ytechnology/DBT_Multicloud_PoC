{% macro get_source_dataset_definition(modelname) -%}
    {{ return(adapter.dispatch('get_source_dataset_definition')(modelname)) }}
{%- endmacro %}

{% macro default__get_source_dataset_definition(modelname) %}
    {% set sql_statement %}
        SELECT SourceDataset FROM REF.SourceDatasetDefinitions 
            WHERE TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}

{% macro fabric__get_source_dataset_definition(modelname) %}
    {% set sql_statement %}
        SELECT SourceDataset FROM REF.SourceDatasetDefinitions 
            WHERE TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}

{% macro databricks__get_source_dataset_definition(modelname) %}
    {% set sql_statement %}
        select SourceDataset from ref.SourceDatasetDefinitions 
            where TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}
