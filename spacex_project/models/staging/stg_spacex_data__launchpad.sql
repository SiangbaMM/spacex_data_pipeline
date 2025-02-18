
{{ config(
    alias = 'vw_stg_spacex_data_launchpad'
    )
}}

with launchpads as
(
    select
	    launchpad_id as launchpad_id,
	    name as launchpad_name,
	    full_name as launchpad_full_name,
	    status as launchpad_status,
	    locality as launchpad_locality,
	    region as launchpad_region,
	    timezone as launchpad_timezone,
	    nullif(latitude, -999999999) as launchpad_latitude,
	    nullif(longitude, -999999999) as launchpad_longitude,
	    nullif(launch_attempts, -999999999) as launchpad_launch_attempts,
	    nullif(launch_successes, -999999999) as launchpad_launch_successes,
	    rockets as launchpad_rockets,
	    launches as launchpad_launches,
	    details as launchpad_details,
	    images as launchpad_images,
        created_at as launchpad_created_at,
	    raw_data as launchpad_raw_data

    from {{ source('stg_spacex_data', 'stg_spacex_data_launchpad') }}
)

select * from launchpads
