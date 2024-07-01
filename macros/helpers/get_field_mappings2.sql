{% macro get_field_mappings2(tablename) -%}
    {{ return(adapter.dispatch('get_field_mappings2')(tablename)) }}
{%- endmacro %}

{%- macro default__get_field_mappings2(tablename) -%}
    {%- set sql_statement = "select ('(' + MappingExpression + ') as ' + TargetField) as CreateColumn from REF.FieldMappingDefinitions where TargetModel = '" + tablename + "' order by Sequence" -%}
    {%- set results = run_query(sql_statement) -%}
    {%- if execute -%}
        {%- set results_list = results.columns[0].values() -%}
    {%- else -%}
        {%- set results_list = [] -%}
    {%- endif -%}
    {%- for result in results_list -%}
        {%- if not loop.first %}
            ,{{ result }}
        {%- else %}
             {{ result }}
        {%- endif -%}
    {%- endfor %}
{%- endmacro %}

{%- macro fabric__get_field_mappings2(tablename) -%}
    {%- set sql_statement = "select ('(' + MappingExpression + ') as ' + TargetField) as CreateColumn from REF.FieldMappingDefinitions where TargetModel = '" + tablename + "' order by Sequence" -%}
    {%- set results = run_query(sql_statement) -%}
    {%- if execute -%}
        {%- set results_list = results.columns[0].values() -%}
    {%- else -%}
        {%- set results_list = [] -%}
    {%- endif -%}
    {%- for result in results_list -%}
        {%- if not loop.first %}
            ,{{ result }}
        {%- else %}
             {{ result }}
        {%- endif -%}
    {%- endfor %}
{%- endmacro %}

{%- macro databricks__get_field_mappings2(tablename) -%}
    {%- set sql_statement = "select ('(' || MappingExpression || ') as ' || TargetField) as CreateColumn from REF.FieldMappingDefinitions where TargetModel = '" + tablename + "' order by Sequence" -%}
    {%- set results = run_query(sql_statement) -%}
    {%- if execute -%}
        {%- set results_list = results.columns[0].values() -%}
    {%- else -%}
        {%- set results_list = [] -%}
    {%- endif -%}
    {%- for result in results_list -%}
        {%- if not loop.first %}
                        ,{{ result }}
        {%- else %}
                         {{ result }}
        {%- endif -%}
    {%- endfor %}
{%- endmacro %}
