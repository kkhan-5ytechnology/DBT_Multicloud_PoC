{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(4) }}",
    tags=["internal"],
) }}

select 'Processing for thread 4' as Message
