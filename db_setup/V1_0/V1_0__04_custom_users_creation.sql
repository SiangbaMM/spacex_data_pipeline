use role spacex_data_dev_secadmin;

drop user if exists spacex_data_dev_load_user;

drop user if exists spacex_data_dev_transform_user;

drop role if exists spacex_data_dev_load_role;

drop role if exists spacex_data_dev_transform_role;

create user if not exists spacex_data_dev_load_user password = '<must_change_password>' login_name = 'spacex_data_dev_load_user' display_name = 'sp_load_user' first_name = 'load' middle_name = 'user' last_name = 'spacex data' must_change_password = true type = person comment = 'User for loading workload';

create user if not exists spacex_data_dev_transform_user password = '<must_change_password>' login_name = 'spacex_data_dev_transform_user' display_name = 'sp_transform_user' first_name = 'transform' middle_name = 'user' last_name = 'spacex data' must_change_password = true type = person comment = 'User for transformation workload';

--Users and roles
use role spacex_data_dev_secadmin;

create role if not exists spacex_data_dev_load_role comment = "Role used for loading workload";

grant role spacex_data_dev_load_role to user spacex_data_dev_load_user;

alter user spacex_data_dev_load_user
set
    default_warehouse = 'spacex_data_dev_load_wh';

--Objects roles 
create role if not exists spacex_data_dev_transform_role comment = "Role used for transformation workload";

grant role spacex_data_dev_transform_role to user spacex_data_dev_transform_user;

alter user spacex_data_dev_transform_user
set
    default_warehouse = 'spacex_data_dev_transform_wh';
