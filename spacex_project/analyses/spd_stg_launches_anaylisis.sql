use database spacex_data_dev;

use schema spd_stg_launches_dev;

use warehouse spacex_data_dev_transform_wh;

--List all successful launches.
with
    success_launches as (
        select
            id,
            name,
            flight_number,
            rocket,
            details,
            date_utc,
            upcoming,
            _sdc_extracted_at,
            _sdc_received_at,
            _sdc_batched_at,
            _sdc_deleted_at,
            _sdc_sequence,
            _sdc_table_version,
            _sdc_sync_started_at
        from
            stg_launches
        where
            details like any (
                '%success%',
                '%successful%',
                '%Success%',
                '%Successful%'
            )
    )
    --Order by date in descending order
select
    sl.id,
    sl.name,
    sl.date_utc,
    sl.flight_number,
    sl.rocket,
    sl.details,
    sl.upcoming
from
    success_launches sl
order by
    sl.date_utc desc;

--Count the number of launches per rocket.
select
    rocket,
    count(rocket)
from
    stg_launches
group by
    rocket;

--Calculating success rates
with
    success_launches as (
        select
            id,
            name,
            flight_number,
            rocket,
            details,
            date_utc,
            upcoming
        from
            stg_launches
        where
            details like any (
                '%success%',
                '%successful%',
                '%Success%',
                '%Successful%'
            )
    )
select
    sl.rocket,
    count(sl.rocket)
from
    success_launches sl
group by
    rocket;

--Finding launches per year
select
    year (date_utc) as launch_year,
    count(*) as launch_number
from
    stg_launches
group by
    launch_year;
