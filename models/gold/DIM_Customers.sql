{{ config(
    tags=["gold"]
) }}

-- depends_on: {{ ref('HUB_Customers') }}

select * from {{ this }}
