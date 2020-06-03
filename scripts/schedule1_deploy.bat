rem ====================================================
rem Schedule-1: Deploy the DATA_AGGREGATOR
rem ----------------------------------------------------
rem Example: schedule1_deploy.bat BI_TEST DATA_AGGREGATOR
rem ====================================================
@echo off
if [%1]==[] goto missDb
if [%2]==[] goto missSchema

@echo Deploying the data aggregator to %1.%2
snowsql ^
--config ..\..\config\.snowsql\config ^
-f .\schedule1_deploy.sql ^
-o exit_on_error=true ^
-o quiet=true ^
-o friendly=true ^
-D db_name=%1 ^
-D sc_name=%2
@echo The data aggregator is deployed to %1.%2
goto done

:missDb
@echo First argument for DB name is missing!
goto example

:missSchema
@echo Second argument for SCHEMA name is missing!

:example
@echo Example: schedule1_deploy.bat BI_TEST _CONTROL_LOGIC

:done
