{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_company'
    ) 
}}

with company as 
(
    select 
	    id as id,
	    name as name,
	    founder as founder,
	    founded as founded,
	    employees as employees,
	    vehicles as vehicles,
	    launch_sites as launch_sites,
	    test_sites as test_sites,
	    ceo as ceo,
	    cto as cto,
	    coo as coo,
	    cto_propulsion as cto_propulsion,
	    valuation as valuation,
	    headquarters as headquarters,
	    links as links,
	    summary as summary,
	    created_at as created_at,
	    updated_at as updated_at,
	    raw_data as raw_data,
	    _sdc_extracted_at as sdc_extracted_at,
        _sdc_received_at as sdc_received_at,
        _sdc_batched_at as sdc_batched_at,
        _sdc_deleted_at as sdc_deleted_at,
        _sdc_sequence as sdc_sequence,
        _sdc_table_version as sdc_table_version,
        _sdc_sync_started_at as sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_company') }}
)

select * from company
