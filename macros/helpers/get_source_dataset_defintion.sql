{% macro get_source_dataset_definition(modelname) %}
    {% set sql_statement %}
        select SourceDataset from {{ ref('SourceDatasetDefinitions') }} 
            where TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") %}
	{{ return(result) }}
{% endmacro %}