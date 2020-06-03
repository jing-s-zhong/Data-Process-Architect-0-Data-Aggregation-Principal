!set variable_substitution=true;
use database &{db_name};
use schema &{sc_name};
--
----------------------------------------
-- Contract auto renew monthly
----------------------------------------
-- ALTER TASK SELLSIDE_MANUAL_ENTRY_MONTHLY_SETUP SUSPEND;
-- DROP TASK SELLSIDE_MANUAL_ENTRY_MONTHLY_SETUP;
--
-- Create root task with a schedule
--CREATE OR REPLACE TASK SELLSIDE_MANUAL_ENTRY_MONTHLY_SETUP
--    WAREHOUSE = S1_BI
--    SCHEDULE = 'USING CRON 59 23 L * * UTC'
--AS
--CALL SELLSIDE_CONTRACT_MANUAL_ENTRY_MONTHLY_SETUP (TO_VARCHAR(CURRENT_DATE()+1));
--
-- Enable the taks scheduled monthly
--ALTER TASK SELLSIDE_MANUAL_ENTRY_MONTHLY_SETUP RESUME;
--
----------------------------------------
-- Contract fulfilling daily
----------------------------------------
-- ALTER TASK SELLSIDE_MANUAL_ENTRY_DAILY_UPDATE SUSPEND;
-- DROP TASK SELLSIDE_MANUAL_ENTRY_DAILY_UPDATE;
--
-- Create root task with a schedule
--CREATE OR REPLACE TASK SELLSIDE_MANUAL_ENTRY_DAILY_UPDATE
--    WAREHOUSE = S1_BI
--    SCHEDULE = 'USING CRON 5 0 * * * UTC'
--AS
--CALL SELLSIDE_CONTRACT_MANUAL_ENTRY_DAILY_UPDATE(TO_VARCHAR(CURRENT_DATE()));
--
-- Enable the root taks for hourly scheduled
--ALTER TASK SELLSIDE_MANUAL_ENTRY_DAILY_UPDATE RESUME;
--
--
SHOW TASKS;
