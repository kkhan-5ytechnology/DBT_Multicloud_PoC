{{ config(
    tags=["gold"]
) }}

-- depends_on: {{ ref('SystemDefinitions') }}
-- depends_on: {{ ref('VaultEntityDefinitions') }}
-- depends_on: {{ ref('SAT_FinancialActivity_CO001') }}
-- depends_on: {{ ref('SAT_FinancialActivity_CO002') }}
-- depends_on: {{ ref('SAT_FinancialActivity_CO003') }}

select * from {{ this }}
