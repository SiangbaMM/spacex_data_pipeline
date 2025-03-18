use role spacex_data_prd_secadmin;

-- role creation
create role if not exists _pbl_spd_prd_sr comment = 'Role with read privileges on SpaceX data published schema of the development environment';

create role if not exists _pbl_spd_prd_sw comment = 'Role with write privileges on SpaceX data published schema of the development environment';

create role if not exists _pbl_spd_prd_sfull comment = 'Role with full privileges on SpaceX data published schema of the development environment';

-- schema management
use role spacex_data_prd_sysadmin;

use database spacex_data_prd;

create schema if not exists pbl_spacex_data
with
    managed access comment = 'SpaceX data launches published schema';

--_pbl_spd_prd_sr
grant usage on database spacex_data_prd to role _pbl_spd_prd_sr;

grant usage on schema pbl_spacex_data to role _pbl_spd_prd_sr;

grant
select
    on all tables in schema pbl_spacex_data to role _pbl_spd_prd_sr;

grant
select
    on future tables in schema pbl_spacex_data to role _pbl_spd_prd_sr;

--_pbl_spd_prd_sw
grant usage on database spacex_data_prd to role _pbl_spd_prd_sw;

grant usage on schema pbl_spacex_data to role _pbl_spd_prd_sw;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema pbl_spacex_data to role _pbl_spd_prd_sw;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema pbl_spacex_data to role _pbl_spd_prd_sw;

grant
create table
    on schema pbl_spacex_data to role _pbl_spd_prd_sw;

grant create materialized view on schema pbl_spacex_data to role _pbl_spd_prd_sw;

grant
create view
    on schema pbl_spacex_data to role _pbl_spd_prd_sw;

--_pbl_spd_prd_sfull
grant usage on database spacex_data_prd to role _pbl_spd_prd_sfull;

grant usage on schema pbl_spacex_data to role _pbl_spd_prd_sfull;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema pbl_spacex_data to role _pbl_spd_prd_sfull;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema pbl_spacex_data to role _pbl_spd_prd_sfull;

grant create schema on database spacex_data_prd to role _pbl_spd_prd_sfull;

grant create file format on all schemas in database spacex_data_prd to role _pbl_spd_prd_sfull;

grant create function on schema pbl_spacex_data to role _pbl_spd_prd_sfull;

grant create pipe on schema pbl_spacex_data to role _pbl_spd_prd_sfull;

grant create procedure on schema pbl_spacex_data to role _pbl_spd_prd_sfull;

grant create stage on schema pbl_spacex_data to role _pbl_spd_prd_sfull;

--grant all privileges on schema pbl_spacex_data to role _pbl_spd_prd_sfull;
-- Grant roles privileges
use role spacex_data_prd_secadmin;

--- Role hierachy
grant role _pbl_spd_prd_sr to role _pbl_spd_prd_sw;

grant role _pbl_spd_prd_sw to role _pbl_spd_prd_sfull;

--- spacex_data_prd_load_role
grant role _pbl_spd_prd_sr to role spacex_data_prd_load_role;

--- spacex_data_prd_transform_role
grant role _pbl_spd_prd_sfull to role spacex_data_prd_transform_role;
