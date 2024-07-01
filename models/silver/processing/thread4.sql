{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(4) }}"
) }}

select 'Processing for thread 4' as Message
