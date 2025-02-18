{%- set schema_name = generate_schema_name(this.schema) -%}

{{
    config(
        alias='pbl_spacex_data_dim_history',
        unique_key='history_surrogate_key'
    )
}}

with source_history as (
    select *
    from {{ ref('stg_spacex_data__history') }}
),

current_history as (
    {% if adapter.get_relation(this.database, schema_name, this.table) is not none %}
        select *
        from {{ ref('pbl_spacex_data_dim__history') }}
        where is_current
    {% else %}
        select null as history_surrogate_key,
               null as history_id,
               null as history_title,
               null as history_event_date_utc,
               null as history_event_date_unix,
               null as history_details,
               null as history_link_article_url,
               null as history_link_reddit_url,
               null as history_link_wikipedia_url,
               null as history_created_at,
               null as dbt_updated_at,
               null::boolean as is_current,
               null::timestamp as valid_from,
               null::timestamp as valid_to
        where false
    {% endif %}
),

change_history as (
    select
        source.*,
        case
            when cur.history_surrogate_key is null
                then 'INSERT'
            when (
                source.history_title != cur.history_title or
                source.history_event_date_utc != cur.history_event_date_utc or
                source.history_event_date_unix != cur.history_event_date_unix or
                source.history_details != cur.history_details or
                source.history_link:article::string != cur.history_link_article_url or
                source.history_link:reddit::string != cur.history_link_reddit_url or
                source.history_link:wikipedia::string != cur.history_link_wikipedia_url
            )
                then 'UPDATE'
            else 'NO_CHANGE'
        end as change_type

    from source_history as source
        left join current_history as cur
            on source.history_id = cur.history_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['history_id', 'history_title']) }} as history_surrogate_key,
        history_id,
        history_title,
        history_event_date_utc,
        history_event_date_unix,
        history_details,
        history_link:article::string as history_link_article_url,
        history_link:reddit::string as history_link_reddit_url,
        history_link:wikipedia::string as history_link_wikipedia_url,
        history_created_at,
        current_timestamp() as dbt_updated_at,
        True as is_current,
        current_timestamp() as valid_from,
        '9999-12-31'::timestamp as valid_to

    from change_history

    where change_type in ('INSERT', 'UPDATE')

    union all

    select
        history_surrogate_key,
        history_id,
        history_title,
        history_event_date_utc,
        history_event_date_unix,
        history_details,
        history_link_article_url,
        history_link_reddit_url,
        history_link_wikipedia_url,
        history_created_at,
        dbt_updated_at,
        is_current,
        valid_from,
        valid_to

    from current_history

    where history_id not in (select history_id from change_history where change_type in ('UPDATE', 'INSERT'))
)

select * from final
