{% macro rebuild_gold_layer() -%}
    {{ return(adapter.dispatch('rebuild_gold_layer')()) }}
{%- endmacro %}

{% macro default__rebuild_gold_layer() %}

{%- endmacro %}

{% macro fabric__rebuild_gold_layer() %}

{%- endmacro %}

{% macro databricks__rebuild_gold_layer() %}
    {# -- PROCESS ALL SEMANTIC MODEL ENTITIES -- #}
    {%- set sql_statement -%}
        select EntityId, DimOrFact from ref.VaultEntityDefinitions where DimOrFact in ('DIM','FACT')
    {%- endset -%}
    {%- set results1 = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result1 in results1 -%}
        {%- set entityid = result1.values()[0] -%}
        {%- set dimorfact = result1.values()[1] -%}
        {%- set target = dimorfact ~ "_" ~ entityid -%}

        {# -- UNION DATA FROM ALL SATS -- #}
        {%- set sql_view = namespace(ddl="") -%}
        {%- set sql_statement -%}
            select SystemId from ref.SystemDefinitions 
        {%- endset -%}
        {%- set results2 = run_query(sql_statement) -%}
        {%- for result2 in results2 -%}
            {%- set systemid = result2.values()[0] -%}
            {%- set sat = "SAT_" ~ entityid ~ "_" ~ systemid -%}
            {%- set csat = "CSAT_" ~ entityid ~ "_" ~ systemid -%}
            
            {# -- GET LATEST SAT INFORMATION -- #}
            {%- if loop.first %}
                {%- set tmp1 -%}
                create or replace view info.{{ target }}
                as 
                select 
                        {{ get_field_mappings2(target) }}

                    from raw.{{ sat }}_ActiveOnly as sat
                    {% if check_table_exists(csat) == '[FOUND]' %}
                        left join raw.{{ csat }} as csat on sat.HashKey = csat.HashKey
                    {% endif %}
                {%- endset -%}
                {%- set sql_view.ddl = sql_view.ddl ~ tmp1 -%}
            {%- else -%}
                {% set tmp1 %}
            
                union all
                select 
                        {{ get_field_mappings2(target) }}

                    from raw.{{ sat }}_ActiveOnly as sat
                    {% if check_table_exists(csat) == '[FOUND]' %}
                        left join raw.{{ csat }} as csat on sat.HashKey = csat.HashKey
                    {% endif %} 
                {%- endset -%}
                {%- set sql_view.ddl = sql_view.ddl ~ tmp1 -%}
            {%- endif -%}

            {%- if loop.last %}
                {{ print("Attempting operation ----->\n" ~ sql_view.ddl) }}
                {% do run_query(sql_view.ddl) %}
            {%- endif -%}
        {%- endfor %} 
    {%- endfor %} 
{%- endmacro %}
