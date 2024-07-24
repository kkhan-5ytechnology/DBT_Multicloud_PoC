{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Vendors_CO003') }}

select * from {{ this }}
