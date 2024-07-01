{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(2) }}",
    tags=["internal"],
) }}

select 'Processing for thread 2' as Message
