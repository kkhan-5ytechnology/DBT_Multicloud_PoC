{{ config(
    tags=["gold"]
) }}

-- depends_on: {{ ref('SystemDefinitions') }}
-- depends_on: {{ ref('VaultEntityDefinitions') }}
-- depends_on: {{ ref('SAT_Customers_CO001') }}
-- depends_on: {{ ref('SAT_Customers_CO003') }}
-- depends_on: {{ ref('SAT_Customers_CO003') }}

select * from {{ this }}
