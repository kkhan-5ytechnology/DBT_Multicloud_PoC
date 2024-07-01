{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(2) }}"
) }}

select 'Processing for thread 2' as Message
