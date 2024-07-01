{% macro rebuild_gold_layer() -%}
    {{ return(adapter.dispatch('rebuild_gold_layer')()) }}
{%- endmacro %}

{% macro default__rebuild_gold_layer() %}

{%- endmacro %}

{% macro fabric__rebuild_gold_layer() %}

{%- endmacro %}

{% macro databricks__rebuild_gold_layer() %}
    
{%- endmacro %}
