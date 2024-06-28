{% snapshot SAT_Customers_SYS001 %}

{{
    config(
      target_schema= 'RAW',
      unique_key='HashKey',
      strategy='check',
      check_cols=['HashDiff'],
    )
}}
select * from {{ ref('SRC_Customers_SYS001') }}
{% endsnapshot %}