{{
    config(
        materialized='incremental',
        unique_key='HashKey'
    )
}}
select 
         a.[HashKey]
        ,a.[Customer_HashKey]
        ,a.[dbt_updated_at]
        
    from {{ ref('HUB_Orders')}} a
        inner join {{ ref('HUB_Customers')}} b on a.[Customer_HashKey] = b.[HashKey]
    {% if is_incremental() %}
    where a.[dbt_updated_at] > (select max([dbt_updated_at]) from {{ this }})
    {% endif %}
    