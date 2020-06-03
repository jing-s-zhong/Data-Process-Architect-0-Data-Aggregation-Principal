!set variable_substitution=true;
use database &{db_name};
use schema &{sc_name};
-------------------------------------------------------
-- Create a dummy aggreagtion table
-------------------------------------------------------
--
-- Remove registratered test source
--
DELETE FROM DATA_AGGREGATION_SOURCES;
--
-- Remove registratered test target
--
DELETE FROM DATA_AGGREGATION_TARGETS;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_TARGET_1;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_TARGET_2;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_SOURCE_1;
--
-- Drop the test data
--
DROP TABLE IF EXISTS _TEST_DATA_SOURCE_2;
