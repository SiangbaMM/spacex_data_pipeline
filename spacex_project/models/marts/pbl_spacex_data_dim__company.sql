{{
    config(
        materialized='table'
    )
}}

select
    company_name as company_name,
    company_founder as company_founder,
    company_founding_date as founding_date,
    company_employee_count as company_employee_count,
    company_vehicle_count as company_vehicle_count,
    company_launch_site_count as company_launch_site_count,
    company_test_site_count as company_test_site_count,
    company_ceo as company_ceo,
    company_cto as company_cto,
    company_coo as company_coo,
    company_cto_propulsion as company_cto_propulsion,
    company_valuation as company_valuation,
    company_headquarters:address::string as company_hq_address,
    company_headquarters:city::string as company_hq_city,
    company_headquarters:state::string as company_hq_state,
    company_headquarters:country::string as company_hq_country,
    company_links:website::string as company_website_url,
    company_links:flickr::string as company_flickr_url,
    company_links:twitter::string as company_twitter_url,
    company_links:elon_twitter::string as company_elon_twitter_url,
    company_sdc_extracted_at as company_sdc_extracted_at,
    company_created_at as company_created_at,
	company_updated_at as company_updated_at

from {{ ref('stg_spacex_data__company') }}
