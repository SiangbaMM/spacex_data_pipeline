use role sysadmin;

use database spacex_data_dev;

create schema if not exists spd_launches
with
    managed access comment = 'SpaceX data launches schema';

--_spd_launches_sr
grant usage on schema spd_launches to role _spd_launches_sr;

grant
select
    on all tables in schema spd_launches to role _spd_launches_sr;

grant
select
    on future tables in schema spd_launches to role _spd_launches_sr;

--_spd_launches_sw
grant usage on schema spd_launches to role _spd_launches_sw;

grant all privileges on all tables in schema spd_launches to role _spd_launches_sw;

grant create file format on all schemas in database spacex_data_dev to role _spd_launches_sw;

grant create function on schema spd_launches to role _spd_launches_sw;

grant create pipe on schema spd_launches to role _spd_launches_sw;

grant create procedure on schema spd_launches to role _spd_launches_sw;

grant create stage on schema spd_launches to role _spd_launches_sw;

grant
create table
    on schema spd_launches to role _spd_launches_sw;

grant create materialized view on schema spd_launches to role _spd_launches_sw;

grant
create view
    on schema spd_launches to role _spd_launches_sw;

--_spd_launches_sfull
grant usage on schema spd_launches to role _spd_launches_sfull;

grant create schema on database spacex_data_dev to role _spd_launches_sfull;

grant
create table
    on schema spd_launches to role _spd_launches_sw;
