use role sysadmin;

-- Étape 1 : Création des rôles spécifiques pour chaque base de données
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS spacex_data_prd_admin;

CREATE ROLE IF NOT EXISTS spacex_data_prd_secadmin;

CREATE ROLE IF NOT EXISTS spacex_data_prd_sysadmin;

-- Hiérarchie des rôles
GRANT ROLE spacex_data_prd_secadmin TO ROLE spacex_data_prd_admin;

GRANT ROLE spacex_data_prd_sysadmin TO ROLE spacex_data_prd_admin;

GRANT ROLE spacex_data_prd_sysadmin TO ROLE SYSADMIN;

GRANT ROLE spacex_data_prd_secadmin TO ROLE SYSADMIN;

-- Étape 2 : Création des bases de données
USE ROLE SYSADMIN;

CREATE DATABASE IF NOT EXISTS spacex_data_prd COMMENT = 'SpaceX Data database of development environment';

-- Étape 3 : Création des warehouses standards pour chaque base de données
CREATE WAREHOUSE IF NOT EXISTS spacex_data_prd_load_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 10 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE COMMENT = 'Space X warehouse for loading purpose';

CREATE WAREHOUSE IF NOT EXISTS spacex_data_prd_transform_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 10 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE COMMENT = 'Space X warehouse for transforming purpose';

CREATE WAREHOUSE IF NOT EXISTS spacex_data_prd_ad_hoc_wh
WITH
    WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 10 AUTO_RESUME = TRUE INITIALLY_SUSPENDED = TRUE COMMENT = 'Space X warehouse for analysis queries';

-- Étape 4 : Attribution des privilèges pour 'spacex_data_dev'
USE ROLE SECURITYADMIN;

GRANT CREATE USER,
CREATE ROLE ON ACCOUNT TO ROLE spacex_data_prd_secadmin;

GRANT USAGE,
MONITOR ON DATABASE spacex_data_prd TO ROLE spacex_data_prd_secadmin;

GRANT CREATE SCHEMA ON DATABASE spacex_data_prd TO ROLE spacex_data_prd_sysadmin
WITH
    MANAGED ACCESS;

USE ROLE SYSADMIN;

GRANT OWNERSHIP ON DATABASE spacex_data_prd TO ROLE spacex_data_prd_sysadmin REVOKE CURRENT GRANTS;

-- Attribution des privilege sur les warehouse
GRANT OWNERSHIP ON WAREHOUSE spacex_data_prd_load_wh TO ROLE spacex_data_prd_sysadmin REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON WAREHOUSE spacex_data_prd_transform_wh TO ROLE spacex_data_prd_sysadmin REVOKE CURRENT GRANTS;

GRANT OWNERSHIP ON WAREHOUSE spacex_data_prd_ad_hoc_wh TO ROLE spacex_data_prd_sysadmin REVOKE CURRENT GRANTS;

-- Étape 7 : Vérification des privilèges
SHOW GRANTS ON DATABASE spacex_data_dev;
