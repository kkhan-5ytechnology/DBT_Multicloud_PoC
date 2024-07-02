-- depends_on: {{ ref('HUB_Customers') }}
-- depends_on: {{ ref('HUB_Orders') }}

select * from {{ this }}
