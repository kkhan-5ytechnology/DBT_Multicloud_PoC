{% macro rebuild_bronze_layer() -%}
    {{ return(adapter.dispatch('rebuild_bronze_layer')()) }}
{%- endmacro %}

{% macro default__rebuild_bronze_layer() %}
    {%- set sql_statement = "SELECT [TargetModel] FROM [REF].[SourceDatasetDefinitions]" -%}
    {%- set results = run_query(sql_statement) -%}
    
    {%- for result in results -%}
        {%- set sql_statement -%}
            IF OBJECT_ID('STG.{{ result.values()[0] }}', 'V') IS NOT NULL
                DROP VIEW STG.{{ result.values()[0] }}
        {%- endset -%}
        {{ print("Attempting operation ----->\n" ~ sql_statement) }}
        {% do run_query(sql_statement) %}
        {%- set sql_statement -%}
            CREATE VIEW STG.{{ result.values()[0] }}
            AS
            SELECT 
                    {{ get_field_mappings2(result.values()[0]) }}
                    
                FROM {{ get_source_dataset_definition(result.values()[0]) }}
                WHERE {{ get_source_dataset_filter(result.values()[0]) }}

        {%- endset -%}
        {{ print("Attempting operation ----->\n" ~ sql_statement) }}
        {% do run_query(sql_statement) %}
    {%- endfor %}
{% endmacro %}

{% macro fabric__rebuild_bronze_layer() %}
    {%- set sql_statement = "SELECT [TargetModel] FROM [REF].[SourceDatasetDefinitions]" -%}
    {%- set results = run_query(sql_statement) -%}
    
    {%- for result in results -%}
        {%- set sql_statement -%}
            IF OBJECT_ID('STG.{{ result.values()[0] }}', 'V') IS NOT NULL
                DROP VIEW STG.{{ result.values()[0] }}
        {%- endset -%}
        {{ print("Attempting operation ----->\n" ~ sql_statement) }}
        {% do run_query(sql_statement) %}
        {%- set sql_statement -%}
            CREATE VIEW STG.{{ result.values()[0] }}
            AS
            SELECT 
                    {{ get_field_mappings2(result.values()[0]) }}
                    
                FROM {{ get_source_dataset_definition(result.values()[0]) }}
                WHERE {{ get_source_dataset_filter(result.values()[0]) }}

        {%- endset -%}
        {{ print("Attempting operation ----->\n" ~ sql_statement) }}
        {% do run_query(sql_statement) %}
    {%- endfor %}
{% endmacro %}

{% macro databricks__rebuild_bronze_layer() %}
    {%- set sql_statement = "select TargetModel from ref.SourceDatasetDefinitions" -%}
    {%- set results = run_query(sql_statement) -%}
    
    {%- for result in results -%}
        {%- set sql_statement -%}
            create or replace view stg.{{ result.values()[0] }}
            as
            select 
                    {{ get_field_mappings2(result.values()[0]) }}
                    
                from {{ get_source_dataset_definition(result.values()[0]) }}
                where {{ get_source_dataset_filter(result.values()[0]) }}
                
        {%- endset -%}
        {{ print("Attempting operation ----->\n" ~ sql_statement) }}
        {% do run_query(sql_statement) %}
    {%- endfor %}
{% endmacro %}
