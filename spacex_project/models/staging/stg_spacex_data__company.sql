{{ config(
    alias = 'vw_stg_spacex_data_company'
    )
}}

with company as
(
    select
	    id as company_id,
	    name as company_name,
	    founder as company_founder,
	    founded as company_founding_date,
	    employees as company_employee_count,
	    vehicles as company_vehicle_count,
	    launch_sites as company_launch_site_count,
	    test_sites as company_test_site_count,
	    ceo as company_ceo,
	    cto as company_cto,
	    coo as company_coo,
	    cto_propulsion as company_cto_propulsion,
	    valuation as company_valuation,
	    headquarters as company_headquarters,
	    links as company_links,
	    summary as company_summary,
	    created_at as company_created_at,
	    updated_at as company_updated_at,
	    raw_data as company_raw_data,
	    _sdc_extracted_at as company_sdc_extracted_at,
        _sdc_received_at as company_sdc_received_at,
        _sdc_batched_at as company_sdc_batched_at,
        _sdc_deleted_at as company_sdc_deleted_at,
        _sdc_sequence as company_sdc_sequence,
        _sdc_table_version as company_sdc_table_version,
        _sdc_sync_started_at as company_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_company') }}
)

select * from company
