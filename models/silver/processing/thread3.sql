{{ config(
    materialized='view',
    post_hook="{{ rebuild_silver_sats(3) }}"
) }}

select 'Processing for thread 3' as Message
