!set variable_substitution=true;
use database &{db_name};
use schema &{sc_name};
--
-------------------------------------------------------
-- Remove installer created objects
-------------------------------------------------------
DROP PROCEDURE IF EXISTS &{db_name}.&{sc_name}.DATA_AGGREGATOR(STRING, BOOLEAN);
DROP PROCEDURE IF EXISTS &{db_name}.&{sc_name}.DATA_AGGREGATOR(STRING, STRING, BOOLEAN);
DROP FUNCTION IF EXISTS &{db_name}.&{sc_name}.REVENUE_SHARE(VARIANT, VARCHAR);
DROP FUNCTION IF EXISTS &{db_name}.&{sc_name}.REVENUE_SHARE(VARIANT, VARCHAR, FLOAT);
DROP FUNCTION IF EXISTS &{db_name}.&{sc_name}.COLUMN_MAP(ARRAY);
DROP FUNCTION IF EXISTS &{db_name}.&{sc_name}.DATA_PATTERN(ARRAY);
DROP TABLE IF EXISTS &{db_name}.&{sc_name}.DATA_AGGREGATION_SOURCES;
DROP TABLE IF EXISTS &{db_name}.&{sc_name}.DATA_AGGREGATION_TARGETS;
--
--DROP SCHEMA IF EXISTS &{db_name}.&{sc_name} RESTRICT;
