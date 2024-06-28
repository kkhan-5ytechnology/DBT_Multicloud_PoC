{%- macro get_field_mappings_safe(tablename, checktablename) -%}
    {%- set sql_statement = "select TargetField, ('(' + MappingExpression + ') AS ' + TargetField) AS CreateColumn from REF.FieldMappingDefinitions where TargetModel = '" + tablename + "' order by Sequence" -%}
    {%- set results = run_query(sql_statement) -%}
    {%- for result in results -%}
        {%- set checktable = ref(checktablename) -%}
        {%- set escaped_result = result.values()[1] | replace("'", "''") -%}
        {%- if check_table_exists('C' + checktablename) == '[FOUND]' -%}
            {%- set sql_statement = "begin try exec('select top 1 " + escaped_result + " from " + checktable.schema + "." + checktable.name + " a inner join " + checktable.schema + ".C" + checktable.name + " b on a.[HashKey] = b.[dbt_hashkey]') end try begin catch select '[NOT FOUND]' as [Result] end catch" -%} 
        {%- else %}
            {%- set sql_statement = "begin try exec('select top 1 " + escaped_result + " from " + checktable.schema + "." + checktable.name + "') end try begin catch select '[NOT FOUND]' as [Result] end catch" -%} 
        {%- endif -%}
        {%- set column_ok = dbt_utils.get_single_value(sql_statement, default="[NOT FOUND]") -%}
        {%- if column_ok == '[NOT FOUND]' -%}
            {%- if not loop.first %}
            ,NULL AS {{ result.values()[0] }}
            {%- else %}
             NULL AS {{ result.values()[0] }}
            {%- endif -%}
        {%- else %}
            {%- if not loop.first %}
            ,{{ result.values()[1] }}
            {%- else %}
             {{ result.values()[1] }}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endmacro %}