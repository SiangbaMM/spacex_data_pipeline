use role spacex_data_dev_secadmin;

-- role creation 
create role if not exists _cmp_spd_dev_sr comment = 'Role with read privileges on SpaceX data compute schema of the development environment';

create role if not exists _cmp_spd_dev_sw comment = 'Role with write privileges on SpaceX data compute schema of the development environment';

create role if not exists _cmp_spd_dev_sfull comment = 'Role with full privileges on SpaceX data compute schema of the development environment';

-- schema management
use role spacex_data_dev_sysadmin;

use database spacex_data_dev;

create schema if not exists cmp_spacex_data
with
    managed access comment = 'SpaceX data launches compute schema';

--_cmp_spd_dev_sr
grant usage on database spacex_data_dev to role _cmp_spd_dev_sr;

grant usage on schema cmp_spacex_data to role _cmp_spd_dev_sr;

grant
select
    on all tables in schema cmp_spacex_data to role _cmp_spd_dev_sr;

grant
select
    on future tables in schema cmp_spacex_data to role _cmp_spd_dev_sr;

--_cmp_spd_dev_sw
grant usage on database spacex_data_dev to role _cmp_spd_dev_sw;

grant usage on schema cmp_spacex_data to role _cmp_spd_dev_sw;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema cmp_spacex_data to role _cmp_spd_dev_sw;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema cmp_spacex_data to role _cmp_spd_dev_sw;

grant
create table
    on schema cmp_spacex_data to role _cmp_spd_dev_sw;

grant create materialized view on schema cmp_spacex_data to role _cmp_spd_dev_sw;

grant
create view
    on schema cmp_spacex_data to role _cmp_spd_dev_sw;

--_cmp_spd_dev_sfull
grant usage on database spacex_data_dev to role _cmp_spd_dev_sfull;

grant usage on schema cmp_spacex_data to role _cmp_spd_dev_sfull;

grant
select
,
update,
insert,
delete,
truncate on all tables in schema cmp_spacex_data to role _cmp_spd_dev_sfull;

grant
select
,
update,
insert,
delete,
truncate on future tables in schema cmp_spacex_data to role _cmp_spd_dev_sfull;

grant create schema on database spacex_data_dev to role _cmp_spd_dev_sfull;

grant create file format on all schemas in database spacex_data_dev to role _cmp_spd_dev_sfull;

grant create function on schema cmp_spacex_data to role _cmp_spd_dev_sfull;

grant create pipe on schema cmp_spacex_data to role _cmp_spd_dev_sfull;

grant create procedure on schema cmp_spacex_data to role _cmp_spd_dev_sfull;

grant create stage on schema cmp_spacex_data to role _cmp_spd_dev_sfull;

--grant all privileges on schema cmp_spacex_data to role _cmp_spd_dev_sfull;
-- Grant roles privileges
use role spacex_data_dev_secadmin;

--- Role hierachy
grant role _cmp_spd_dev_sr to role _cmp_spd_dev_sw;

grant role _cmp_spd_dev_sw to role _cmp_spd_dev_sfull;

--- spacex_data_dev_load_role
grant role _cmp_spd_dev_sr to role spacex_data_dev_load_role;

--- spacex_data_dev_transform_role
grant role _cmp_spd_dev_sfull to role spacex_data_dev_transform_role;

revoke all grants on schema stg_spacex_data
from
    role _cmp_spd_dev_sr;

revoke all grants on schema stg_spacex_data
from
    role _cmp_spd_dev_sw;

revoke all grants on schema stg_spacex_data
from
    role _cmp_spd_dev_sfull;
