{% macro incremental_tmp_table_dropper(RelationObject) %}
    {% set tmpTableName %}
        {{ '"' + RelationObject.database + '"."' + RelationObject.schema + '"."' + RelationObject.identifier + '__dbt_tmp"'}}
    {% endset %}
    {% set query %}
        drop table if exists {{tmpTableName}};
    {% endset %}
    {{ return(query) }}
{% endmacro %}