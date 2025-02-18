{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_company',
        unique_key='company_surrogate_key'
    )
}}

with source_company as (
    select *
    from {{ ref('stg_spacex_data__company') }}
),

current_company as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_company') }}
        where is_current
    {% else %}
        select null as company_surrogate_key,
               null as company_id,
               null as company_name,
               null as company_founder,
               null as company_founding_date,
               null as company_employee_count,
               null as company_vehicle_count,
               null as company_launch_site_count,
               null as company_test_site_count,
               null as company_ceo,
               null as company_cto,
               null as company_coo,
               null as company_cto_propulsion,
               null as company_valuation,
               null as company_hq_address,
               null as company_hq_city,
               null as company_hq_state,
               null as company_hq_country,
               null as company_website_url,
               null as company_flickr_url,
               null as company_twitter_url,
               null as company_elon_twitter_url,
               null as company_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_company as (
    select
        source.*,
        case
            when cur.company_surrogate_key is null
                then 'INSERT'
            when (
                source.company_name != cur.company_name or
                source.company_founder != cur.company_founder or
                source.company_founding_date != cur.company_founding_date or
                source.company_employee_count != cur.company_employee_count or
                source.company_vehicle_count != cur.company_vehicle_count or
                source.company_launch_site_count != cur.company_launch_site_count or
                source.company_test_site_count != cur.company_test_site_count or
                source.company_ceo != cur.company_ceo or
                source.company_cto != cur.company_cto or
                source.company_coo != cur.company_coo or
                source.company_cto_propulsion != cur.company_cto_propulsion or
                source.company_valuation != cur.company_valuation
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_company as source
        left join current_company as cur
            on source.company_id = cur.company_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['company_name', 'company_founder', 'company_founding_date']) }} as company_surrogate_key,
        company_id,
        company_name,
        company_founder,
        company_founding_date,
        company_employee_count,
        company_vehicle_count,
        company_launch_site_count,
        company_test_site_count,
        company_ceo,
        company_cto,
        company_coo,
        company_cto_propulsion,
        company_valuation,
        company_headquarters:address::string as company_hq_address,
        company_headquarters:city::string as company_hq_city,
        company_headquarters:state::string as company_hq_state,
        company_headquarters:country::string as company_hq_country,
        company_links:website::string as company_website_url,
        company_links:flickr::string as company_flickr_url,
        company_links:twitter::string as company_twitter_url,
        company_links:elon_twitter::string as company_elon_twitter_url,
        company_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_company

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        company_surrogate_key,
        company_id,
        company_name,
        company_founder,
        company_founding_date,
        company_employee_count,
        company_vehicle_count,
        company_launch_site_count,
        company_test_site_count,
        company_ceo,
        company_cto,
        company_coo,
        company_cto_propulsion,
        company_valuation,
        company_hq_address,
        company_hq_city,
        company_hq_state,
        company_hq_country,
        company_website_url,
        company_flickr_url,
        company_twitter_url,
        company_elon_twitter_url,
        company_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_company

    where company_id not in (select company_id from change_company where change_type in ('UPDATE', 'INSERT'))
)

select * from final
