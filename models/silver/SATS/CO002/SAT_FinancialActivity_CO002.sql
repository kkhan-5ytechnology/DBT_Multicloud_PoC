{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_FinancialActivity_CO002') }}

select * from {{ this }}
