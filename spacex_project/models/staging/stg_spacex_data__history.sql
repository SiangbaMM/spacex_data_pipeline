
{{ config(
    alias = 'vw_stg_spacex_data_history'
    )
}}

with history as
(
    select
        history_id as history_id,
	    title as history_title,
	    event_date_utc as history_event_date_utc,
	    nullif(event_date_unix, -999999999) as history_event_date_unix,
	    details as history_details,
	    try_parse_json(links) as history_link,
	    nullif(flight_number, -999999999) as history_flight_number,
        created_at as history_created_at,
	    raw_data as history_raw_data

    from {{ source('stg_spacex_data', 'stg_spacex_data_history') }}
)

select * from history
