{% macro get_source_dataset_filter(modelname) %}
    {% set sql_statement %}
        select WhereClause from {{ ref('SourceDatasetDefinitions') }} 
            where TargetModel = '{{ modelname }}'
    {% endset %}
    {% set result = dbt_utils.get_single_value(sql_statement, default="1 = 1") %}
	{{ return(result) }}
{% endmacro %}