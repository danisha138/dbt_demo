{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='sync_all_columns'
    )
}}

with
customer as (
    select ...
)

{% if is_incremental() %}
,diff as (
    select * from customer 
    except
    select * exclude (_last_modified)
    from {{ this }}
)
{% endif %}

select 
    *,
    current_timestamp() as _last_modified
from customer 

{% if is_incremental() %}
where exists (
    select 1
    from diff
    where diff.id = cruise.id
)
{% endif %}