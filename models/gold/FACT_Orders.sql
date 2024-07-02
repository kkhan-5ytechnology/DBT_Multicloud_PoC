{{ config(
    tags=["gold"]
) }}

-- depends_on: {{ ref('HUB_Orders') }}

select * from {{ this }}
