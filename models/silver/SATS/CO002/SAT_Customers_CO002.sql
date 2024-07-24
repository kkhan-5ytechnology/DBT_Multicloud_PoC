{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Customers_CO002') }}

select * from {{ this }}
