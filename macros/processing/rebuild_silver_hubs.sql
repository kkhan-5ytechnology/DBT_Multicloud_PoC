{% macro rebuild_silver_hubs() -%}
    {{ return(adapter.dispatch('rebuild_silver_hubs')()) }}
{%- endmacro %}

{% macro default__rebuild_silver_hubs() %}

{%- endmacro %}

{% macro fabric__rebuild_silver_hubs() %}

{%- endmacro %}

{% macro databricks__rebuild_silver_hubs() %}
    {# -- PROCESS ALL ENTITIES -- #}
    {%- set sql_statement -%}
        select EntityId from ref.vaultentitydefintions 
    {%- endset -%}
    {%- set results1 = run_query(sql_statement) -%}
    {%- for result1 in results1 -%}
        {%- set entityid = result1.values()[0] -%}
        {%- set target = "HUB_" ~ entityid -%}

        {# -- NEED ONE HUB PER ENTITY SET -- #}
        {%- set sql_view = namespace(ddl="") -%}
        {%- set sql_statement -%}
            select SystemId from ref.systemdefinitions 
        {%- endset -%}
        {%- set results2 = run_query(sql_statement) -%}
        {%- for result2 in results2 -%}
            {%- set systemid = result2.values()[0] -%}
            {%- set source = "SAT_" ~ entityid ~ "_" ~ systemid -%}
            {%- if loop.first %}
                {%- set tmp1 -%}
                create or replace view raw.{{ target }}
                as 
                select 
                        {{ get_field_mappings2(target) }}

                    from raw.{{ source }} 
                    where VaultVersion = 1
                    
                {%- endset -%}
                {%- set sql_view.ddl = sql_view.ddl ~ tmp1 -%}
            {%- else -%}
                {% set tmp1 %}
            
                union all
                select 
                        {{ get_field_mappings2(target) }}

                    from raw.{{ source }} 
                    where VaultVersion = 1
                    
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
