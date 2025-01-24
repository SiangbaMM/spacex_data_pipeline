
{{ config(
    materialized='table',
    alias = 'stg_launches'
    ) 
}}

with launches as 
(
    select 
        id ,
        name ,
        flight_number ,
        rocket ,
        details ,
        date_utc ,
        upcoming ,
        _sdc_extracted_at ,
        _sdc_received_at ,
        _sdc_batched_at ,
        _sdc_deleted_at ,
        _sdc_sequence ,
        _sdc_table_version ,
        _sdc_sync_started_at
    
    from {{ source('spd_launches_dev', 'launches') }}
)

select * from launches
