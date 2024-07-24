{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Customers_CO001') }}

select * from {{ this }}
