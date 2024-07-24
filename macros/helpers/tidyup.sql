{% macro tidyup() %}
    {% set query %}
        drop view if exists dbo.thread1
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        drop view if exists dbo.thread2
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        drop view if exists dbo.thread3
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        drop view if exists dbo.thread4
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}