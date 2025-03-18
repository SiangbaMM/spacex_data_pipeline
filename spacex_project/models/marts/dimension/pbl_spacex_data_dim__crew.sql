{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_crew',
        unique_key='crew_surrogate_key'
    )
}}

with source_crew as (
    select *
    from {{ ref('stg_spacex_data__crew') }}
),

current_crew as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ source('pbl_spacex_data', 'pbl_spacex_data_dim_crew') }}
        where is_current
    {% else %}
        select null as crew_surrogate_key,
               null as crew_id,
               null as crew_name,
               null as crew_agency,
               null as crew_image_url,
               null as crew_wikipedia_url,
               null as crew_status,
               null as crew_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_crew as (
    select
        source.*,
        case
            when cur.crew_surrogate_key is null
                then 'INSERT'
            when (
                source.crew_name != cur.crew_name or
                source.crew_agency != cur.crew_agency or
                source.crew_status != cur.crew_status or
                source.crew_image != cur.crew_image_url or
                source.crew_wikipedia != cur.crew_wikipedia_url
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_crew as source
        left join current_crew as cur
            on source.crew_id = cur.crew_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['crew_id', 'crew_name']) }} as crew_surrogate_key,
        crew_id,
        crew_name,
        crew_agency,
        crew_image as crew_image_url,
        crew_wikipedia as crew_wikipedia_url,
        crew_status,
        crew_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_crew

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        crew_surrogate_key,
        crew_id,
        crew_name,
        crew_agency,
        crew_image_url,
        crew_wikipedia_url,
        crew_status,
        crew_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_crew

    where crew_id not in (select crew_id from change_crew where change_type in ('UPDATE', 'INSERT'))
)

select * from final
