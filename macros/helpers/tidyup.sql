{% macro tidyup() %}
    {% set query %}
        drop view if exists RAW.thread1
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        drop view if exists RAW.thread2
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        drop view if exists RAW.thread3
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
        drop view if exists RAW.thread4
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}