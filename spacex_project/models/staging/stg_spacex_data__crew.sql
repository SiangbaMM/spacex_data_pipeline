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
	    updated_at as crew_updated_at,
	    raw_data as crew_raw_data,
	    _sdc_extracted_at as crew_sdc_extracted_at,
        _sdc_received_at as crew_sdc_received_at,
        _sdc_batched_at as crew_sdc_batched_at,
        _sdc_deleted_at as crew_sdc_deleted_at,
        _sdc_sequence as crew_sdc_sequence,
        _sdc_table_version as crew_sdc_table_version,
        _sdc_sync_started_at as crew_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_crew') }}
)

select * from crew
