{%- set src = ['SAT_Customers_SYS001'] -%}

-- depends_on: {{ ref('CSAT_Customers_SYS001') }}

{{ sql_for_stage3_dims(src) }}