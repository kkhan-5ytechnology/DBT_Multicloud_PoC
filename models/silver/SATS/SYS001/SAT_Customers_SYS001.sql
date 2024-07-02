{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Customers_SYS001') }}

select * from {{ this }}
