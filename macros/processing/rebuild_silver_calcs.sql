{% macro rebuild_silver_calcs() -%}
    {{ return(adapter.dispatch('rebuild_silver_calcs')()) }}
{%- endmacro %}

{% macro default__rebuild_silver_calcs() %}
    {# -- ALL ENTITIES THAT NEED A SAT -- #}
    {%- set sql_statement -%}
        SELECT EntityId FROM REF.VaultEntityDefinitions WHERE Calculations = 1
    {%- endset -%}
    {%- set results1 = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result1 in results1 -%}
        {%- set entityid = result1.values()[0] -%}
        
        {# -- NEED ONE CSAT PER SAT -- #}
        {%- set sql_statement -%}
            SELECT SystemId FROM REF.SystemDefinitions 
        {%- endset -%}
        {%- set results2 = run_query(sql_statement) -%}
        {%- for result2 in results2 -%}
            {%- set systemid = result2.values()[0] -%}
            {%- set source = "SAT_" ~ entityid ~ "_" ~ systemid -%}
            {%- set target = "CSAT_" ~ entityid ~ "_" ~ systemid -%}

            {# -- DROP CSAT TABLE IF EXISTS -- #}
            {%- if check_table_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    DROP TABLE RAW.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {%- set sql_statement -%}
                CREATE TABLE RAW.{{ target }}
                AS
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultCalculated

                    FROM RAW.{{ source }}

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
        {%- endfor %} 
    {%- endfor %} 
{%- endmacro %}

{% macro fabric__rebuild_silver_calcs() %}
    {# -- ALL ENTITIES THAT NEED A SAT -- #}
    {%- set sql_statement -%}
        SELECT EntityId FROM REF.VaultEntityDefinitions WHERE Calculations = 1
    {%- endset -%}
    {%- set results1 = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result1 in results1 -%}
        {%- set entityid = result1.values()[0] -%}
        
        {# -- NEED ONE CSAT PER SAT -- #}
        {%- set sql_statement -%}
            SELECT SystemId FROM REF.SystemDefinitions 
        {%- endset -%}
        {%- set results2 = run_query(sql_statement) -%}
        {%- for result2 in results2 -%}
            {%- set systemid = result2.values()[0] -%}
            {%- set source = "SAT_" ~ entityid ~ "_" ~ systemid -%}
            {%- set target = "CSAT_" ~ entityid ~ "_" ~ systemid -%}

            {# -- DROP CSAT TABLE IF EXISTS -- #}
            {%- if check_table_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    DROP TABLE RAW.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {%- set sql_statement -%}
                CREATE TABLE RAW.{{ target }}
                AS
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultCalculated

                    FROM RAW.{{ source }}

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
        {%- endfor %} 
    {%- endfor %} 
{%- endmacro %}

{% macro databricks__rebuild_silver_calcs() %}
    {# -- ALL ENTITIES THAT NEED A SAT -- #}
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
