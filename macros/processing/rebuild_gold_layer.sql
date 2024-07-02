{% macro rebuild_gold_layer() -%}
    {{ return(adapter.dispatch('rebuild_gold_layer')()) }}
{%- endmacro %}

{% macro default__rebuild_gold_layer() %}

{%- endmacro %}

{% macro fabric__rebuild_gold_layer() %}

{%- endmacro %}

{% macro databricks__rebuild_gold_layer() %}
    {# -- PROCESS ALL ENTITIES THAT NEED TO GO INTO SEMANTIC MODEL -- #}
    {%- set sql_statement -%}
        select EntityId from ref.VaultEntityDefinitions where Calculations = 1
    {%- endset -%}
    {%- set results1 = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result1 in results1 -%}
        {%- set entityid = result1.values()[0] -%}
        
        {# -- NEED ONE CSAT PER SAT -- #}
        {%- set sql_statement -%}
            select SystemId from ref.SystemDefinitions 
        {%- endset -%}
        {%- set results2 = run_query(sql_statement) -%}
        {%- for result2 in results2 -%}
            {%- set systemid = result2.values()[0] -%}
            {%- set source = "SAT_" ~ entityid ~ "_" ~ systemid -%}
            {%- set target = "CSAT_" ~ entityid ~ "_" ~ systemid -%}

            {%- set sql_statement -%}
                create or replace table raw.{{ target }}
                as
                select 
                        {{ get_field_mappings2(target) }}
                        ,cast('{{ stopwatch_start }}' as timestamp) as VaultCalculated

                    from raw.{{ source }}

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
        {%- endfor %} 
    {%- endfor %} 





{%- endmacro %}
