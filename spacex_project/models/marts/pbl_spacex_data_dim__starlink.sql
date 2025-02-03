{{
    config(
        materialized='incremental',
        alias='dim__starlink',
        unique_key='starlink_id'
    )
}}

select
    starlink_id as starlink_id,
    starlink_launch_id as starlink_launch_id,
    starlink_longitude as starlink_longitude,
    starlink_latitude as starlink_latitude,
    starlink_height_km as starlink_height_km,
    starlink_velocity_kms as starlink_velocity_kms,
    starlink_spaceTrack:OBJECT_NAME::string as starlink_spaceTrack_object_name,
    starlink_spaceTrack:LATITUDE::float as starlink_spaceTrack_latitude,
    starlink_spaceTrack:LONGITUDE::float as starlink_spaceTrack_longitude,
    starlink_spaceTrack:HEIGHT_KM::float as starlink_spaceTrack_height_km,
    starlink_spaceTrack:VELOCITY_KMS::float as starlink_spaceTrack_velocity_kms,
    starlink_sdc_extracted_at as starlink_sdc_extracted_at,
    starlink_created_at as starlink_created_at,
	starlink_updated_at as starlink_updated_at
        
from {{ ref('stg_spacex_data__starlink') }}

{% if is_incremental() %}
    where starlink_sdc_extracted_at > (select max(starlink_sdc_extracted_at) from {{ this }})
{% endif %}
