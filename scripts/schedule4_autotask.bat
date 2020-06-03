rem ====================================================
rem Schedule-4: Scheule the DATA_AGGREGATOR
rem ----------------------------------------------------
rem Example: schedule4_autotask.bat BI_TEST DATA_AGGREGATOR
rem ====================================================
@echo off
if [%1]==[] goto missDb
if [%2]==[] goto missSchema

@echo Creating auto scheule tasks for the data aggregator in %1.%2
snowsql ^
--config ..\..\config\.snowsql\config ^
-f .\schedule4_autotask.sql ^
-o exit_on_error=true ^
-o quiet=true ^
-o friendly=true ^
-D db_name=%1 ^
-D sc_name=%2
@echo The data aggregator in %1.%2 is scheduled by the auto-tasks
goto done

:missDb
@echo First argument for DB name is missing!
goto example

:missSchema
@echo Second argument for SCHEMA name is missing!

:example
@echo Example: schedule4_autotask.bat BI_TEST _CONTROL_LOGIC

:done
