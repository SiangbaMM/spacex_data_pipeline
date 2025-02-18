use role spacex_data_dev_secadmin;

-- role creation
create role if not exists _stg_spd_dev_sr comment = 'Role with read privileges on SpaceX data staging schema of the development environment';

create role if not exists _stg_spd_dev_sw comment = 'Role with write privileges on SpaceX data staging schema of the development environment';

create role if not exists _stg_spd_dev_sfull comment = 'Role with full privileges on SpaceX data staging schema of the development environment';

-- schema management
use role spacex_data_dev_sysadmin;

use database spacex_data_dev;

create schema if not exists stg_spacex_data
with
    managed access comment = 'SpaceX data launches staging schema';

--_stg_spd_dev_sr
grant usage on database spacex_data_dev to role _stg_spd_dev_sr;

grant usage on schema stg_spacex_data to role _stg_spd_dev_sr;

grant
select
    on all tables in schema stg_spacex_data to role _stg_spd_dev_sr;

grant
select
    on future tables in schema stg_spacex_data to role _stg_spd_dev_sr;

--_stg_spd_dev_sw
grant usage on database spacex_data_dev to role _stg_spd_dev_sw;

grant usage on schema stg_spacex_data to role _stg_spd_dev_sw;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema stg_spacex_data to role _stg_spd_dev_sw;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema stg_spacex_data to role _stg_spd_dev_sw;

grant
create table
    on schema stg_spacex_data to role _stg_spd_dev_sw;

grant create materialized view on schema stg_spacex_data to role _stg_spd_dev_sw;

grant
create view
    on schema stg_spacex_data to role _stg_spd_dev_sw;

--_stg_spd_dev_sfull
grant usage on database spacex_data_dev to role _stg_spd_dev_sfull;

grant usage on schema stg_spacex_data to role _stg_spd_dev_sfull;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema stg_spacex_data to role _stg_spd_dev_sfull;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema stg_spacex_data to role _stg_spd_dev_sfull;

grant create schema on database spacex_data_dev to role _stg_spd_dev_sfull;

grant create file format on all schemas in database spacex_data_dev to role _stg_spd_dev_sfull;

grant create function on schema stg_spacex_data to role _stg_spd_dev_sfull;

grant create pipe on schema stg_spacex_data to role _stg_spd_dev_sfull;

grant create procedure on schema stg_spacex_data to role _stg_spd_dev_sfull;

grant create stage on schema stg_spacex_data to role _stg_spd_dev_sfull;

--grant all privileges on schema stg_spacex_data to role _stg_spd_dev_sfull;
-- Grant roles privileges
use role spacex_data_dev_secadmin;

--- Role hierachy
grant role _stg_spd_dev_sr to role _stg_spd_dev_sw;

grant role _stg_spd_dev_sw to role _stg_spd_dev_sfull;

--- spacex_data_dev_load_role
grant role _stg_spd_dev_sfull to role spacex_data_dev_load_role;

--- spacex_data_dev_transform_role
grant role _stg_spd_dev_sr to role spacex_data_dev_transform_role;

--- spacex_data_dev_sysadmin
grant role _stg_spd_dev_sfull to role spacex_data_dev_sysadmin;
