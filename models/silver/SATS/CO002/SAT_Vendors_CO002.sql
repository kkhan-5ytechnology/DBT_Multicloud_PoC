{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Vendors_CO002') }}

select * from {{ this }}
