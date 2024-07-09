{% macro rebuild_silver_sats(thread) -%}
    {{ return(adapter.dispatch('rebuild_silver_sats')(thread)) }}
{%- endmacro %}

{% macro default__rebuild_silver_sats(thread) %}
    {# -- NEED ONE SAT PER SOURCE -- #}
    {%- set sql_statement -%}
        SELECT TargetModel FROM REF.SourceDatasetDefinitions WHERE Thread = {{ thread }}
    {%- endset -%}
    {%- set results = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result in results -%}
        {%- set source = result.values()[0] -%}
        {%- set target = result.values()[0] | replace("SRC_", "SAT_") -%}
        {{ print("Processing " ~ target ~ " on thread " ~ thread) }}

        {# -- DATAVAULT NOT ACTIVE -- #}
        {%- if var("vault_active") == False -%}

            {# -- DROP SAT TABLE IF EXISTS -- #}
            {%- if check_table_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    DROP TABLE RAW.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {# -- INITIALISE VIEW -- #}
            {%- set sql_statement -%}
                if OBJECT_ID('RAW.{{ target }}', 'V') IS NOT NULL
                    DROP VIEW RAW.{{ target }}
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            {%- set sql_statement -%}
                CREATE VIEW RAW.{{ target }}
                AS
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,1 AS VaultVersion
                        ,'Passthru - vault inactive' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) as VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) as VaultEffectiveTo

                    FROM STG.{{ source }} AS src
                    
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {%- set sql_statement -%}
                if OBJECT_ID('RAW.{{ target }}_ActiveOnly', 'V') IS NOT NULL
                    DROP VIEW RAW.{{ target }}
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            {%- set sql_statement -%}
                CREATE VIEW RAW.{{ target }}_ActiveOnly
                AS
                SELECT 
                        *
                        
                    FROM RAW.{{ target }}
                    WHERE VaultEffectiveTo IS NULL

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            
        {% else %}
        {# -- DATAVAULT ACTIVE -- #}

            {# -- DROP SAT VIEW IF EXISTS -- #}
            {%- if check_view_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    DROP VIEW RAW.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {# -- INITIALISE TABLE -- #}
            {%- if check_table_exists(target) != '[FOUND]' -%}
                {%- set sql_statement -%}
                CREATE TABLE RAW.{{ target }}
                AS
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,1 as VaultVersion
                        ,'Initial load - active in vault' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) AS VaultEffectiveTo

                    FROM STG.{{ source }} AS src

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

                {%- set sql_statement -%}
                    if OBJECT_ID('RAW.{{ target }}_ActiveOnly', 'V') IS NOT NULL
                        DROP VIEW RAW.{{ target }}_ActiveOnly
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
                {%- set sql_statement -%}
                    CREATE VIEW RAW.{{ target }}_ActiveOnly
                    AS
                    SELECT 
                            *
                            
                        FROM RAW.{{ target }}
                        WHERE VaultEffectiveTo IS NULL

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

            {% endif %}

            {# -- ADD NEW RECS TO VAULT -- #}
            {%- set sql_statement -%}
                INSERT INTO RAW.{{ target }}
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,(1 + COALESCE((select MAX(VaultVersion) FROM RAW.{{ target }} x WHERE x.HashKey = src.HashKey),0)) AS VaultVersion
                        ,'Inserted - active in vault' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) AS VaultEffectiveTo

                    FROM STG.{{ source }} AS src
                        FULL JOIN RAW.{{ target }} AS sat ON src.HashKey = sat.HashKey AND sat.VaultEffectiveTo IS NULL 
                    WHERE src.HashKey <> COALESCE(sat.HashKey,'')

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- EXPIRE DELETED RECS IN VAULT -- #}
            {%- set sql_statement -%}
                UPDATE RAW.{{ target }}
                        SET VaultEffectiveTo = '{{ stopwatch_start }}', VaultState = 'Deleted - expired in vault'
                    WHERE VaultEffectiveTo IS NULL AND HashKey NOT IN (SELECT HashKey FROM STG.{{ source }})

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- EXPIRE UPDATED RECS IN VAULT -- #}
            {%- set sql_statement -%}
                UPDATE sat
                        SET sat.VaultEffectiveTo = '{{ stopwatch_start }}', sat.VaultState = 'Updated - versioned in vault'
                    FROM RAW.{{ target }} AS sat 
                        INNER JOIN STG.{{ source }} AS src ON sat.HashKey = src.HashKey AND sat.VaultEffectiveTo IS NULL AND sat.HashDiff <> src.HashDiff

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- ADD NEW VERSIONS TO VAULT -- #}
            {%- set sql_statement -%}
                INSERT INTO RAW.{{ target }}
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,(1 + COALESCE((SELECT MAX(VaultVersion) FROM RAW.{{ target }} x WHERE x.HashKey = src.HashKey),0)) AS VaultVersion
                        ,'Inserted - active in vault' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) AS VaultEffectiveTo

                    FROM STG.{{ source }} AS src
                        FULL JOIN RAW.{{ target }} AS sat ON src.HashKey = sat.HashKey AND sat.VaultEffectiveTo IS NULL 
                    WHERE src.HashKey <> COALESCE(sat.HashKey,'')

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
        {% endif %}
    {%- endfor %}
{%- endmacro %}

{% macro fabric__rebuild_silver_sats(thread) %}
    {# -- NEED ONE SAT PER SOURCE -- #}
    {%- set sql_statement -%}
        SELECT TargetModel FROM REF.SourceDatasetDefinitions WHERE Thread = {{ thread }}
    {%- endset -%}
    {%- set results = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result in results -%}
        {%- set source = result.values()[0] -%}
        {%- set target = result.values()[0] | replace("SRC_", "SAT_") -%}
        {{ print("Processing " ~ target ~ " on thread " ~ thread) }}

        {# -- DATAVAULT NOT ACTIVE -- #}
        {%- if var("vault_active") == False -%}

            {# -- DROP SAT TABLE IF EXISTS -- #}
            {%- if check_table_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    DROP TABLE RAW.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {# -- INITIALISE VIEW -- #}
            {%- set sql_statement -%}
                if OBJECT_ID('RAW.{{ target }}', 'V') IS NOT NULL
                    DROP VIEW RAW.{{ target }}
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            {%- set sql_statement -%}
                CREATE VIEW RAW.{{ target }}
                AS
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,1 AS VaultVersion
                        ,'Passthru - vault inactive' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) as VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) as VaultEffectiveTo

                    FROM STG.{{ source }} AS src
                    
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {%- set sql_statement -%}
                if OBJECT_ID('RAW.{{ target }}_ActiveOnly', 'V') IS NOT NULL
                    DROP VIEW RAW.{{ target }}
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            {%- set sql_statement -%}
                CREATE VIEW RAW.{{ target }}_ActiveOnly
                AS
                SELECT 
                        *
                        
                    FROM RAW.{{ target }}
                    WHERE VaultEffectiveTo IS NULL

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            
        {% else %}
        {# -- DATAVAULT ACTIVE -- #}

            {# -- DROP SAT VIEW IF EXISTS -- #}
            {%- if check_view_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    DROP VIEW RAW.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {# -- INITIALISE TABLE -- #}
            {%- if check_table_exists(target) != '[FOUND]' -%}
                {%- set sql_statement -%}
                CREATE TABLE RAW.{{ target }}
                AS
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,1 as VaultVersion
                        ,'Initial load - active in vault' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) AS VaultEffectiveTo

                    FROM STG.{{ source }} AS src

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

                {%- set sql_statement -%}
                    if OBJECT_ID('RAW.{{ target }}_ActiveOnly', 'V') IS NOT NULL
                        DROP VIEW RAW.{{ target }}_ActiveOnly
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
                {%- set sql_statement -%}
                    CREATE VIEW RAW.{{ target }}_ActiveOnly
                    AS
                    SELECT 
                            *
                            
                        FROM RAW.{{ target }}
                        WHERE VaultEffectiveTo IS NULL

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

            {% endif %}

            {# -- ADD NEW RECS TO VAULT -- #}
            {%- set sql_statement -%}
                INSERT INTO RAW.{{ target }}
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,(1 + COALESCE((select MAX(VaultVersion) FROM RAW.{{ target }} x WHERE x.HashKey = src.HashKey),0)) AS VaultVersion
                        ,'Inserted - active in vault' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) AS VaultEffectiveTo

                    FROM STG.{{ source }} AS src
                        FULL JOIN RAW.{{ target }} AS sat ON src.HashKey = sat.HashKey AND sat.VaultEffectiveTo IS NULL 
                    WHERE src.HashKey <> COALESCE(sat.HashKey,'')

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- EXPIRE DELETED RECS IN VAULT -- #}
            {%- set sql_statement -%}
                UPDATE RAW.{{ target }}
                        SET VaultEffectiveTo = '{{ stopwatch_start }}', VaultState = 'Deleted - expired in vault'
                    WHERE VaultEffectiveTo IS NULL AND HashKey NOT IN (SELECT HashKey FROM STG.{{ source }})

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- EXPIRE UPDATED RECS IN VAULT -- #}
            {%- set sql_statement -%}
                UPDATE sat
                        SET sat.VaultEffectiveTo = '{{ stopwatch_start }}', sat.VaultState = 'Updated - versioned in vault'
                    FROM RAW.{{ target }} AS sat 
                        INNER JOIN STG.{{ source }} AS src ON sat.HashKey = src.HashKey AND sat.VaultEffectiveTo IS NULL AND sat.HashDiff <> src.HashDiff

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {# -- ADD NEW VERSIONS TO VAULT -- #}
            {%- set sql_statement -%}
                INSERT INTO RAW.{{ target }}
                SELECT 
                        {{ get_field_mappings2(target) }}
                        ,(1 + COALESCE((SELECT MAX(VaultVersion) FROM RAW.{{ target }} x WHERE x.HashKey = src.HashKey),0)) AS VaultVersion
                        ,'Inserted - active in vault' AS VaultState
                        ,CAST('{{ stopwatch_start }}' AS DATETIME2(6)) AS VaultEffectiveFrom
                        ,CAST(NULL AS DATETIME2(6)) AS VaultEffectiveTo

                    FROM STG.{{ source }} AS src
                        FULL JOIN RAW.{{ target }} AS sat ON src.HashKey = sat.HashKey AND sat.VaultEffectiveTo IS NULL 
                    WHERE src.HashKey <> COALESCE(sat.HashKey,'')

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
        {% endif %}
    {%- endfor %}
{%- endmacro %}

{% macro databricks__rebuild_silver_sats(thread) %}
    {# -- NEED ONE SAT PER SOURCE -- #}
    {%- set sql_statement -%}
        select TargetModel from ref.SourceDatasetDefinitions where Thread = {{ thread }}
    {%- endset -%}
    {%- set results = run_query(sql_statement) -%}
    {%- set stopwatch_start = run_started_at.strftime('%Y-%m-%d %H:%M:%S') -%}
    {%- for result in results -%}
        {%- set source = result.values()[0] -%}
        {%- set target = result.values()[0] | replace("SRC_", "SAT_") -%}
        {{ print("Processing " ~ target ~ " on thread " ~ thread) }}

        {# -- DATAVAULT NOT ACTIVE -- #}
        {%- if var("vault_active") == False -%}

            {# -- DROP SAT TABLE IF EXISTS -- #}
            {%- if check_table_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    drop table if exists raw.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {# -- INITIALISE VIEW -- #}
            {%- set sql_statement -%}
                create or replace view raw.{{ target }}
                as
                select 
                        {{ get_field_mappings2(target) }}
                        ,1 as VaultVersion
                        ,'Passthru - vault inactive' as VaultState
                        ,cast('{{ stopwatch_start }}' as timestamp) as VaultEffectiveFrom
                        ,cast(NULL as timestamp) as VaultEffectiveTo

                    from stg.{{ source }} as src
                    
            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}

            {%- set sql_statement -%}
                create or replace view raw.{{ target }}_ActiveOnly
                as
                select 
                        *
                        
                    from raw.{{ target }}
                    where VaultEffectiveTo is null

            {%- endset -%}
            {{ print("Attempting operation ----->\n" ~ sql_statement) }}
            {% do run_query(sql_statement) %}
            
        {% else %}
        {# -- DATAVAULT ACTIVE -- #}

            {# -- DROP SAT VIEW IF EXISTS -- #}
            {%- if check_view_exists(target) == '[FOUND]' -%}
                {%- set sql_statement -%}
                    drop view if exists raw.{{ target }}
                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}
            {%- endif -%}

            {# -- INITIALISE TABLE -- #}
            {%- if check_table_exists(target) != '[FOUND]' -%}
                {%- set sql_statement -%}
                create or replace table raw.{{ target }}
                as
                select 
                        {{ get_field_mappings2(target) }}
                        ,1 as VaultVersion
                        ,'Initial load - active in vault' as VaultState
                        ,cast('{{ stopwatch_start }}' as timestamp) as VaultEffectiveFrom
                        ,cast(NULL as timestamp) as VaultEffectiveTo

                    from stg.{{ source }} as src

                {%- endset -%}
                {{ print("Attempting operation ----->\n" ~ sql_statement) }}
                {% do run_query(sql_statement) %}

                {%- set sql_statement -%}
                    create or replace view raw.{{ target }}_ActiveOnly
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
                        {{ get_field_mappings2(target) }}
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
                        {{ get_field_mappings2(target) }}
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
