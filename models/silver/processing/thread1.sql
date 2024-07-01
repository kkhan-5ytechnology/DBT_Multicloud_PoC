{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(1) }}",
    tags=["internal"],
) }}

select 'Processing for thread 1' as Message
