{{ config(
    tags=["bronze"]
) }}

-- depends_on: {{ ref('SourceDatasetDefinitions') }}

select * from {{ this }}
