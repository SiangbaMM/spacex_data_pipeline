{{ config(
    alias = 'vw_stg_spacex_data_cores'
    )
}}

with cores as
(
    select
	    core_id as core_id,
        serial as core_serial,
        nullif(block, -999999999) as core_block,
        status as core_status,
        nullif(reuse_count, -999999999) as core_reuse_count,
        nullif(rtls_attempts, -999999999) as core_rtls_attempts,
        nullif(rtls_landings, -999999999) as core_rtls_landings,
        nullif(asds_attempts, -999999999) as core_asds_attempts,
        nullif(asds_landings, -999999999) as core_asds_landings,
        last_update as core_last_update,
        launches as core_launch_id,
        created_at as core_created_at,
        updated_at as core_updated_at,
        raw_data as core_raw_data,
	    _sdc_extracted_at as core_sdc_extracted_at,
        _sdc_received_at as core_sdc_received_at,
        _sdc_batched_at as core_sdc_batched_at,
        _sdc_deleted_at as core_sdc_deleted_at,
        _sdc_sequence as core_sdc_sequence,
        _sdc_table_version as core_sdc_table_version,
        _sdc_sync_started_at as core_sdc_sync_started_at

    from {{ source('stg_spacex_data', 'stg_spacex_data_cores') }}
)

select * from cores
