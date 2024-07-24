{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_FinancialActivity_CO003') }}

select * from {{ this }}
