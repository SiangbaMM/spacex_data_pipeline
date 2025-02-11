use database spacex_data_dev;

use schema spd_launches_dev;

use warehouse spacex_data_dev_transform_wh;

--Joining Tables
select
    l.id,
    l.date_utc,
    r.name as rocket_name,
    r.active
from
    launches l
    inner join rockets r on l.rocket = r.id;

--Retrieve the more recent launch
---Using a Subquery
select
    *
from
    launches
where
    date_utc = (
        select
            max(date_utc)
        from
            launches
    );

---Using a CTE
----1
with
    recent_launch as (
        select
            max(date_utc) as max_date_utc
        from
            launches
    )
select
    *
from
    launches l
    join recent_launch rl
where
    l.date_utc = rl.max_date_utc;
