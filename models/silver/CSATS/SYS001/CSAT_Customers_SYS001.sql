{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SAT_Customers_SYS001') }}

select * from {{ this }}
