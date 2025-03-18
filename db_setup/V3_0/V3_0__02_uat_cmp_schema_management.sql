use role spacex_data_uat_secadmin;

-- role creation
create role if not exists _cmp_spd_uat_sr comment = 'Role with read privileges on SpaceX data compute schema of the development environment';

create role if not exists _cmp_spd_uat_sw comment = 'Role with write privileges on SpaceX data compute schema of the development environment';

create role if not exists _cmp_spd_uat_sfull comment = 'Role with full privileges on SpaceX data compute schema of the development environment';

-- schema management
use role spacex_data_uat_sysadmin;

use database spacex_data_uat;

create schema if not exists cmp_spacex_data
with
    managed access comment = 'SpaceX data launches compute schema';

--_cmp_spd_uat_sr
grant usage on database spacex_data_uat to role _cmp_spd_uat_sr;

grant usage on schema cmp_spacex_data to role _cmp_spd_uat_sr;

grant
select
    on all tables in schema cmp_spacex_data to role _cmp_spd_uat_sr;

grant
select
    on future tables in schema cmp_spacex_data to role _cmp_spd_uat_sr;

--_cmp_spd_uat_sw
grant usage on database spacex_data_uat to role _cmp_spd_uat_sw;

grant usage on schema cmp_spacex_data to role _cmp_spd_uat_sw;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema cmp_spacex_data to role _cmp_spd_uat_sw;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema cmp_spacex_data to role _cmp_spd_uat_sw;

grant
create table
    on schema cmp_spacex_data to role _cmp_spd_uat_sw;

grant create materialized view on schema cmp_spacex_data to role _cmp_spd_uat_sw;

grant
create view
    on schema cmp_spacex_data to role _cmp_spd_uat_sw;

--_cmp_spd_uat_sfull
grant usage on database spacex_data_uat to role _cmp_spd_uat_sfull;

grant usage on schema cmp_spacex_data to role _cmp_spd_uat_sfull;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema cmp_spacex_data to role _cmp_spd_uat_sfull;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema cmp_spacex_data to role _cmp_spd_uat_sfull;

grant create schema on database spacex_data_uat to role _cmp_spd_uat_sfull;

grant create file format on all schemas in database spacex_data_uat to role _cmp_spd_uat_sfull;

grant create function on schema cmp_spacex_data to role _cmp_spd_uat_sfull;

grant create pipe on schema cmp_spacex_data to role _cmp_spd_uat_sfull;

grant create procedure on schema cmp_spacex_data to role _cmp_spd_uat_sfull;

grant create stage on schema cmp_spacex_data to role _cmp_spd_uat_sfull;

--grant all privileges on schema cmp_spacex_data to role _cmp_spd_uat_sfull;
-- Grant roles privileges
use role spacex_data_uat_secadmin;

--- Role hierachy
grant role _cmp_spd_uat_sr to role _cmp_spd_uat_sw;

grant role _cmp_spd_uat_sw to role _cmp_spd_uat_sfull;

--- spacex_data_uat_load_role
grant role _cmp_spd_uat_sr to role spacex_data_uat_load_role;

--- spacex_data_uat_transform_role
grant role _cmp_spd_uat_sfull to role spacex_data_uat_transform_role;
