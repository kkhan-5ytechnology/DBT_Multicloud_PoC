{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(3) }}",
    tags=["internal"],
) }}

select 'Processing for thread 3' as Message
