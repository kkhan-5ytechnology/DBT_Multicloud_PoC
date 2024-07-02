-- depends_on: {{ ref('HUB_Customers_SYS001') }}
-- depends_on: {{ ref('HUB_Orders_SYS001') }}

select * from {{ this }}
