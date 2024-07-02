{% macro rebuild_silver_links() -%}
    {{ return(adapter.dispatch('rebuild_silver_links')()) }}
{%- endmacro %}

{% macro default__rebuild_silver_links() %}

{%- endmacro %}

{% macro fabric__rebuild_silver_links() %}

{%- endmacro %}

{% macro databricks__rebuild_silver_links() %}
    {# -- PROCESS ALL ENTITIES -- #}
    {%- set sql_statement -%}
        select EntityId from ref.VaultEntityDefinitions where LinkApplicable = 1
    {%- endset -%}
    {%- set results1 = run_query(sql_statement) -%}
    {%- for result1 in results1 -%}
        {%- set entityid = result1.values()[0] -%}
        {%- set target = "LINK_" ~ entityid -%}

        {# -- NEED ONE LINK PER ENTITY SET -- #}
        {%- set sql_view = namespace(ddl="") -%}
        {%- set sql_statement -%}
            select SystemId from ref.SystemDefinitions 
        {%- endset -%}
        {%- set results2 = run_query(sql_statement) -%}
        {%- for result2 in results2 -%}
            {%- set systemid = result2.values()[0] -%}
            {%- set source = "SAT_" ~ entityid ~ "_" ~ systemid -%}

            {# -- GET LATEST LINK INFORMATION -- #}
            {%- if loop.first %}
                {%- set tmp1 -%}
                create or replace view raw.{{ target }}
                as 
                select 
                        {{ get_field_mappings2(target) }}

                    from raw.{{ source }}_ActiveOnly  
                    
                {%- endset -%}
                {%- set sql_view.ddl = sql_view.ddl ~ tmp1 -%}
            {%- else -%}
                {% set tmp1 %}
            
                union all
                select 
                        {{ get_field_mappings2(target) }}

                    from raw.{{ source }}_ActiveOnly  
                    
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
