{{ config(
    schema = 'cmp_spacex_data',
    alias = 'vw_stg_spacex_data_cores'
    ) 
}}

with cores as 
(
    select 
	    core_id as core_id,
        serial as serial,
        block as block,
        status as status,
        reuse_count as reuse_count,
        rtls_attempts as rtls_attempts,
        rtls_landings as rtls_landings,
        asds_attempts as asds_attempts,
        asds_landings as asds_landings,
        last_update as last_update,
        launches as launches,
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

    from {{ source('stg_spacex_data', 'stg_spacex_data_cores') }}
)

select * from cores
