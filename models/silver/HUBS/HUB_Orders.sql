{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SAT_Orders_SYS001') }}

select * from {{ this }}
