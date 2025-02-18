{{ config(
    alias = 'vw_stg_spacex_data_crew'
    )
}}

with crew as
(
    select
	    crew_id as crew_id,
	    name as crew_name,
	    agency as crew_agency,
	    image as crew_image,
	    wikipedia as crew_wikipedia,
	    status as crew_status,
	    launches as crew_launches,
	    created_at as crew_created_at,
	    raw_data as crew_raw_data

    from {{ source('stg_spacex_data', 'stg_spacex_data_crew') }}
)

select * from crew
