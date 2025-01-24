
{{ config(
    materialized='table',
    alias = 'stg_rocket'
    ) 
}}

with launches as 
(
    select 
        id ,
        name ,
        active
    
    from {{ source('spd_launches_dev', 'rocket') }}
)

select * from launches
