use role spacex_data_dev_secadmin;

-- spacex_data_dev_load_user
create user if not exists spacex_data_dev_load_user password = '<must_change_password>' login_name = 'spacex_data_dev_load_user' display_name = 'sp_load_user' first_name = 'load' middle_name = 'user' last_name = 'spacex data' must_change_password = true type = person comment = 'User for loading workload';

--- Role
create role if not exists spacex_data_dev_load_role comment = "Role used for loading workload";

grant role spacex_data_dev_load_role to user spacex_data_dev_load_user;

--- Warehouse
use role spacex_data_dev_sysadmin;

grant usage,
monitor on warehouse spacex_data_dev_load_wh to role spacex_data_dev_load_role;

use role spacex_data_dev_secadmin;

alter user spacex_data_dev_load_user
set
    default_warehouse = 'spacex_data_dev_load_wh';

--- Database
grant usage on database spacex_data_dev to role spacex_data_dev_load_role;

-- spacex_data_dev_transform_user 
create user if not exists spacex_data_dev_transform_user password = '<must_change_password>' login_name = 'spacex_data_dev_transform_user' display_name = 'sp_transform_user' first_name = 'transform' middle_name = 'user' last_name = 'spacex data' must_change_password = true type = person comment = 'User for transformation workload';

--- Role
create role if not exists spacex_data_dev_transform_role comment = "Role used for transformation workload";

grant role spacex_data_dev_transform_role to user spacex_data_dev_transform_user;

--- Warehouse
use role spacex_data_dev_sysadmin;

grant usage,
monitor on warehouse spacex_data_dev_transform_wh to role spacex_data_dev_transform_role;

use role spacex_data_dev_secadmin;

alter user spacex_data_dev_transform_user
set
    default_warehouse = 'spacex_data_dev_transform_wh';

--- Database
grant usage on database spacex_data_dev to role spacex_data_dev_transform_role;
