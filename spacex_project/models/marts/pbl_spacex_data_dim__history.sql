{{
    config(
        materialized='table',
        unique_key='history_id'
    )
}}

select
    history_id as history_id,
    history_title as history_title,
    history_event_date_utc as history_event_date_utc,
    history_event_date_unix as history_event_date_unix,
    history_details as history_details,
    history_link:article::string as history_link_article_url,
    history_link:reddit::string as history_link_reddit_url,
    history_link:wikipedia::string as history_link_wikipedia_url,
    history_flight_number as history_flight_number,
    history_sdc_extracted_at as history_sdc_extracted_at,
    history_created_at as history_created_at,
	history_updated_at as history_updated_at

from {{ ref('stg_spacex_data__history') }}
