{% macro rebuild_gold_layer() -%}
    {{ adapter.dispatch('rebuild_gold_layer1')() }}
    {{ adapter.dispatch('rebuild_gold_layer2')() }}
    {{ adapter.dispatch('rebuild_gold_layer3')() }}
{%- endmacro %}

{% macro default__rebuild_gold_layer() %}

{%- endmacro %}

{% macro fabric__rebuild_gold_layer() %}

{%- endmacro %}

{% macro databricks__rebuild_gold_layer1() %}
    {{ print("databricks__rebuild_gold_layer1") }}
{%- endmacro %}

{% macro databricks__rebuild_gold_layer2() %}
    {{ print("databricks__rebuild_gold_layer2") }}
{%- endmacro %}

{% macro databricks__rebuild_gold_layer3() %}
    {{ print("databricks__rebuild_gold_layer3") }}
{%- endmacro %}
