{% macro rebuild_silver_layer() -%}
    {{ return(adapter.dispatch('rebuild_silver_layer')()) }}
{%- endmacro %}

{% macro default__rebuild_silver_layer() %}

{%- endmacro %}

{% macro fabric__rebuild_silver_layer() %}

{%- endmacro %}

{% macro databricks__rebuild_silver_layer() %}
    {%- set sql_statement = "select TargetModel from ref.sourcedatasetdefinitions" -%}
    {%- set results = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result in results -%}
        {%- set source = result.values()[0] -%}
        {%- set target = result.values()[0] | replace("SRC_", "SAT_") -%}

        {# -- DATAVAULT NOT ACTIVE -- #}
        {%- if var("vault_active") == False -%}

            {# -- DROP SAT TABLE IF EXISTS -- #}
            {%- set sql_statement -%}
                drop table if exists raw.{{ target }}
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- INITIALISE VIEW -- #}
            {%- set sql_statement -%}
                create or replace view raw.{{ target }}
                as
                select 
                        src.*
                        ,1 as VaultVersion
                        ,'Initial load - active in vault' as VaultState
                        ,cast('{{ stopwatch_start }}' as timestamp) as VaultEffectiveFrom
                        ,cast(NULL as timestamp) as VaultEffectiveTo

                    from stg.{{ source }} as src
                    
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

        {% else %}
        {# -- DATAVAULT ACTIVE -- #}

            {# -- DROP SAT VIEW IF EXISTS -- #}
            {%- set sql_statement -%}
                drop view if exists raw.{{ target }}
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- INITIALISE TABLE -- #}
            {%- if check_table_exists(target) != '[FOUND]' -%}
                {%- set sql_statement -%}
                    create or replace table raw.{{ target }}
                    as
                    select 
                            src.*
                            ,1 as VaultVersion
                            ,'Initial load - active in vault' as VaultState
                            ,cast('{{ stopwatch_start }}' as timestamp) as VaultEffectiveFrom
                            ,cast(NULL as timestamp) as VaultEffectiveTo

                        from stg.{{ source }} as src

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

                {%- set sql_statement -%}
                    create or replace view tmp.{{ target }}_ActiveOnly
                    as
                    select 
                            *
                            
                        from raw.{{ target }}
                        where VaultEffectiveTo is null

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

            {% endif %}

            {# -- ADD NEW RECS TO VAULT -- #}
            {%- set sql_statement -%}
                insert into raw.{{ target }}
                select 
                        src.*
                        ,(1 + coalesce((select max(VaultVersion) from raw.{{ target }} x where x.HashKey = src.HashKey),0)) as VaultVersion
                        ,'Inserted - active in vault' as VaultState
                        ,cast('{{ stopwatch_start }}' as timestamp) as VaultEffectiveFrom
                        ,cast(NULL as timestamp) as VaultEffectiveTo

                    from stg.{{ source }} as src
                        full join raw.{{ target }} as sat on src.HashKey = sat.HashKey and sat.VaultEffectiveTo is null
                    where src.HashKey <> coalesce(sat.HashKey,'')

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- EXPIRE DELETED RECS IN VAULT -- #}
            {%- set sql_statement -%}
                update raw.{{ target }}
                        set VaultEffectiveTo = '{{ stopwatch_start }}', VaultState = 'Deleted - expired in vault'
                    where VaultEffectiveTo is null and HashKey not in (select HashKey from stg.{{ source }})

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- EXPIRE UPDATED RECS IN VAULT -- #}
            {%- set sql_statement -%}
                merge into raw.{{ target }} as sat using stg.{{ source }} as src
                    on sat.HashKey = src.HashKey and sat.VaultEffectiveTo is null and sat.HashDiff <> src.HashDiff
                when matched then
                    update set
                        VaultEffectiveTo = '{{ stopwatch_start }}', 
                        VaultState = 'Updated - versioned in vault'

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- ADD NEW VERSIONS TO VAULT -- #}
            {%- set sql_statement -%}
                insert into raw.{{ target }}
                select 
                        src.*
                        ,(1 + coalesce((select max(VaultVersion) from raw.{{ target }} x where x.HashKey = src.HashKey),0)) as VaultVersion
                        ,'Inserted - active in vault' as VaultState
                        ,cast('{{ stopwatch_start }}' as timestamp) as VaultEffectiveFrom
                        ,cast(NULL as timestamp) as VaultEffectiveTo

                    from stg.{{ source }} as src
                        full join raw.{{ target }} as sat on src.HashKey = sat.HashKey and sat.VaultEffectiveTo is null
                    where src.HashKey <> coalesce(sat.HashKey,'')

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
        {% endif %}
    {%- endfor %}     
{%- endmacro %}
