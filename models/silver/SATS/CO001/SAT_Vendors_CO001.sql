{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_Vendors_CO001') }}

select * from {{ this }}
