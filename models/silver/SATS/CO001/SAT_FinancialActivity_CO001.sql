{{ config(
    tags=["silver"]
) }}

-- depends_on: {{ ref('SRC_FinancialActivity_CO001') }}

select * from {{ this }}
