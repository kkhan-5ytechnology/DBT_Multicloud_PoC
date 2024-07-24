{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Customers_CO003') }}

select * from {{ this }}
