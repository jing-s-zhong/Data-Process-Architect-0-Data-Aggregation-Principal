# Automated Data Aggregation (1): Data Aggregator

Data aggregation is a common data processing method, which is widely used in data analysis, business intelligence and machine learning projects. It is quite often that we need to aggregate the data from many data sources in a very different format, and the data format may change from time to time. It could be a challenge to program a data processing application to handle the varieties of this case. Here we introduce an automation solution that solves the case where all the source data are continuously being loaded into a series of database tables with the different table schemas.

## I. Concept Introduction

Let’s compare our data processing case with an automated color paint product line to extract the key elements and control factors of the system, then use these elements and factors  to model our data aggregation.

![An automated product line](images/figure1-an-automated-product-line.jpg?raw=true "An automated product line")
Figure-1 An automated product line

In the above illustrated product line, we have the raw materials continuously coming from three conveyors S1, S2 and S3. The material from S1 is packed in small bags, it takes four to feed in a mixer and produce one bucket of product; the material from S2 is packed in middle size bags, it takes two to feed a mixer and produce one bucket of product; and the material from S3 is packed in large bags, it just needs one to feed a mixer and produce one bucket of product. Every three different buckets of product from three raw sources will be blended together to produce a bucket of final product.

In contrast to our data processing case, we can think the conveyors are raw data tables, each of them holds the different format of raw data in different granularities. The processing point is the timestamp of the current raw data ingesting. The available point is the timestamp of the committed raw data. The data processing app will chunk the raw data into micro-batches and join the metadata as the transformation to get the unified data in the same format and same granularity, then aggregate the unified data into the needed granularity in the same format and load the result into a warehouse table. We can summarize two entities and their attributes of our case as Table-1 and Table-2.


Table-1 Target data entity and attributes
Attritue | Type | Description
------------------------|----------------|-------------------------------------------------
Target_Table | Property | The summary date table name
Batch_Control_Column | Property | The column is used to chunk the data
Batch_Control_Size | Property | The micro chunk size in a batch processing
Batch_Control_Next | Method | A function to determine the chunk border
Batch_Processed | Property | A timestamp of the completed processing
Batch_Processing | Property | A timestamp of the stop point of current batch
Batch_Microchunk_Current | Property | A timestamp of the chunk in current batch
Batch_Schedule_Type | Property | The minimum schedule frequency
Batch_Schedule_Last | Property | The timestamp of the last schedule
Pattern_Columns | Property | The unified data formatting columns
Groupby_Columns | Property | The aggregation granularity columns
Groupby_Pattern | Property | A bitwise indicators to group-by columns
Groupby_Flexible | Property | Allow the different granularity in result
Aggregate_Columns | Property | The column list which will aggregate values
Aggregate_Functions | Property | The function list applied to aggregate columns



Table-2 Source data entity and attributes
Attritue | Type | Description
------------------------|----------------|-------------------------------------------------
Target_Table | Property | Which summary data needs this source
Source_Table | Property | The source data table name
Pattern_Default | Property | Default granularity of the data in source table
Pattern_Flexible | Property | Allow multi-granularities exist in one table
Data_Available_Time | Property | The timestamp of the committed source data
Data_Check_Schedule | Property | Timestamp of the last check of availability
Transformation | Method | A query or view to refactor the data format


## II. Data Modeling

Based on the previous description, we can easily figure out a very simple data model just having two entities in our case. The entity relationship diagram is illustrated in  Figure-2.


![Entity relationship diagram](images/figure2-entity-relationship-diagram.jpg?raw=true "Entity relationship diagram")
Figure-2 Entity Relationship Diagram


### A. Target Data Definition

```
1.TARGET_LABEL (TEXT):
2.TARGET_TABLE (TEXT):
3.BATCH_CONTROL_COLUMN (TEXT):
4.BATCH_CONTROL_SIZE (NUMBER):
5.BATCH_CONTROL_NEXT (TEXT):
6.BATCH_PROCESSED (TIMESTAMP_NTZ):
7.BATCH_PROCESSING (TIMESTAMP_NTZ):
8.BATCH_MICROCHUNK_CURRENT (TEXT):
9.BATCH_SCHEDULE _TYPE (TEXT):
10.BATCH_SCHEDULE_LAST (TIMESTAMP_NTZ):
11.PATTERN_COLUMNS (ARRAY):
12.GROUPBY_COLUMNS (ARRAY):
13.GROUPBY_PATTERN (NUMBER):
14.GROUPBY_FLEXIBLE (BOOLEAN):
15.AGGREGATE_COLUMNS (ARRAY):
16.AGGREGATE_FUNCTIONS (ARRAY):
17.DEFAULT_PROCEDURE (TEXT):
```


### B. Source Data Definition

```
1.SOURCE_LABEL (TEXT):
2.TARGET_TABLE (TEXT):
3.SOURCE_TABLE (TEXT):
4.SOURCE_ENABLED (BOOLEAN):
5.PATTERN_DEFAULT (NUMBER):
6.PATTERN_FLEXIBLE (BOOLEAN):
7.DATA_AVAILABLETIME (TIMESTAMP_NTZ):
8.DATA_CHECKSCHEDULE (TIMESTAMP_NTZ):
9.TRANSFORMATION (TEXT):
```

## III. Code Implementation

As all raw data have been ingested into a snowflake warehouse stage area already, we will implement the processing with snowflake stored procedures and functions. Snowflake supports a simplified JavaScript API, the functions and procedures can be programmed in JavaScript, so that most JavaScript programmers involve the work quickly with a very short time learning.

### A. Chunk Processor

![Single chunk processing flowchart](images/figure3-single-chunk-processing.jpg?raw=true "Single chunk processing flowchart")

Figure-3 Flowchart for single chunk processing

### B. Batch Processor

![Loop multi-chunks processing](images/figure4-loop-multi-chunks-processing.jpg?raw=true "Loop multi-chunks processing flowchart")

Figure-4 Flowchart for loop multi-chunks processing

## IV. Aggregation Setup

Here we are going to set up two data aggregations as the usage examples. We will aggregate the demo data in 5 minutes batches in demo-1, then we aggregate same data in different way in daily batches. Let's generate some test data for our demo in two source tables "_TEST_DATA_SOURCE_1" and "_TEST_DATA_SOURCE_2" here.

```
--
-- Create dummy aggregation data source 1
--
-- DROP TABLE _TEST_DATA_SOURCE_1;
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_SOURCE_1
AS
SELECT 0::NUMBER DATA_PT,
	DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(1)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ DATA_TS,
    1::NUMBER DATA_I1,
    UNIFORM(0, 15, RANDOM(11))::NUMBER DATA_I2,
    NULLIF(UNIFORM(0, 15, RANDOM(111)),0)::NUMBER DATA_I3,
    RANDSTR(UNIFORM(1, 10, RANDOM()), RANDOM())::VARCHAR DATA_A1,
    RANDSTR(ABS(RANDOM()) % 10, RANDOM())::VARCHAR  DATA_A2,
    NULLIF(RANDSTR(UNIFORM(0, 10, RANDOM()), RANDOM()),'')::VARCHAR  DATA_A3,
    UNIFORM(0, 50, RANDOM(10))::NUMBER VALUE_I1,
    UNIFORM(0, 1500, RANDOM(15))/10::FLOAT VALUE_D1
FROM TABLE(GENERATOR(ROWCOUNT => 50000)) V
ORDER BY 1;
--
UPDATE _TEST_DATA_SOURCE_1
SET DATA_PT = DATA_PATTERN(ARRAY_CONSTRUCT(
    1,
    '_TEST_DATA_SOURCE_1',
    DATE(DATA_TS),
    DATE_PART(HOUR, DATA_TS),
    DATA_TS,
    DATA_I1,
    DATA_I2,
    DATA_I3,
    DATA_A1,
    DATA_A2,
    DATA_A3
));
--
-- Create dummy aggregation data source 2
--
-- DROP TABLE _TEST_DATA_SOURCE_2;
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_SOURCE_2
AS
SELECT 0::NUMBER DATA_PT,
	DATEADD(MINUTE, -UNIFORM(1, 50000, RANDOM(2)), CURRENT_TIMESTAMP(0))::TIMESTAMP_NTZ DATA_TS,
    2::NUMBER DATA_I1,
    UNIFORM(0, 15, RANDOM(22))::NUMBER DATA_I2,
    NULLIF(UNIFORM(0, 15, RANDOM(222)),0)::NUMBER DATA_I3,
    RANDSTR(UNIFORM(1, 10, RANDOM()), RANDOM())::VARCHAR  DATA_A1,
    RANDSTR(ABS(RANDOM()) % 10, RANDOM())::VARCHAR DATA_A2,
    NULLIF(RANDSTR(UNIFORM(0, 10, RANDOM()), RANDOM()),'')::VARCHAR DATA_A3,
    UNIFORM(0, 50, RANDOM(10))::NUMBER VALUE_I1,
    UNIFORM(0, 1500, RANDOM(15))/10::FLOAT VALUE_D1
FROM TABLE(GENERATOR(ROWCOUNT => 50000)) V
ORDER BY 1;
--
UPDATE _TEST_DATA_SOURCE_2
SET DATA_PT = DATA_PATTERN(ARRAY_CONSTRUCT(
    1,
    '_TEST_DATA_SOURCE_2',
    DATE(DATA_TS),
    DATE_PART(HOUR, DATA_TS),
    DATA_TS,
    DATA_I1,
    DATA_I2,
    DATA_I3,
    DATA_A1,
    DATA_A2,
    DATA_A3
));
```

### A. Demo-1: Aggregate data in 5 minute batches


The target table namely “_TEST_DATA_TARGET_1” is created by running the following SQL script, then we go through the listed steps in following sections to complete the full setup.

```
--
-- Create dummy aggregation data target 1
--
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_TARGET_1 (
	"DATA_PT"	NUMBER         NOT NULL,
	"DATA_DN"	VARCHAR,
	"DATA_DT"	DATE           NOT NULL,
	"DATA_HR"	NUMBER,
	"DATA_TS"	TIMESTAMP_NTZ,
	"DATA_I1"	NUMBER         NOT NULL,
	"DATA_I2"	NUMBER,
	"DATA_I3"	NUMBER,
	"DATA_A1"	VARCHAR,
	"DATA_A2"	VARCHAR,
	"DATA_A3"	VARCHAR,
	"VSUM_I1"	NUMBER,
	"VCNT_I2"	NUMBER,
	"VSUM_D1"	FLOAT,
	"VAVG_D2"	FLOAT
);
```

#### 1. Aggregation Target Setup

```
-- Update or add the aggregation target
MERGE INTO DATA_AGGREGATION_TARGETS D
USING (
  SELECT 'Test: Dummy aggregation target 1' TARGET_LABEL
  	,$1 TARGET_TABLE
  	,$2 BATCH_CONTROL_COLUMN
  	,$3 BATCH_CONTROL_SIZE
  	,$4 BATCH_CONTROL_NEXT
  	,DATEADD(HOUR, -3, DATE_TRUNC('HOUR', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP(0)))) BATCH_PROCESSED
  	,NULL BATCH_PROCESSING
  	,NULL BATCH_MICROCHUNK_CURRENT
  	,$5 BATCH_SCHEDULE_TYPE
  	,NULL BATCH_SCHEDULE_LAST
  	,PARSE_JSON($6) PATTERN_COLUMNS
  	,PARSE_JSON($7) GROUPBY_COLUMNS
  	,DATA_PATTERN(PARSE_JSON($8)) GROUPBY_PATTERN
  	,True GROUPBY_FLEXIBLE
  	,PARSE_JSON($9) AGGREGATE_COLUMNS
  	,PARSE_JSON($10) AGGREGATE_FUNCTIONS
  	,NULL DEFAULT_PROCEDURE
  FROM VALUES (
  	'_TEST_DATA_TARGET_1'
  	,'DATA_TS'
  	, 5
  	,'DATEADD(MINUTE, :2, :1)'
  	,'MINUTES'
  	-- all group-by columns in source data
  	,'["DATA_PATTERN",
  		"DATA_NAME",
  		"DATA_DATE",
      "DATA_HOUR",
      "DATA_TIME",
  		"DATA_I1",
  		"DATA_I2",
  		"DATA_I3",
  		"DATA_A1",
  		"DATA_A2",
  		"DATA_A3"
        ]'
  	-- group-by columns of target data and which source column is the match
  	,'["DATA_PT:DATA_PATTERN",
  		"DATA_DN:DATA_NAME",
      "DATA_DT:DATA_DATE",
      "DATA_HR:DATA_HOUR",
      "DATA_TS:DATA_TIME",
  		"DATA_I1:DATA_I1",
  		"DATA_I2:DATA_I2",
  		"DATA_I3:DATA_I3",
  		"DATA_A1:DATA_A1",
  		"DATA_A2:DATA_A2",
  		"DATA_A3:DATA_A3"
         ]'
  	-- indicators of which group-by column are needed in target table
  	,'[1,1,1,1,1,1,1,1,0,0,0]'
  	-- aggregate columns of target data and which aggregating column is the match
  	,'["VSUM_I1:VALUE_I1","VCNT_I2:VALUE_I2","VSUM_D1:VALUE_D1","VAVG_D2:VALUE_D2"]'
  	-- what aggregation function will be used for every aggregation column
  	,'["SUM(?)","COUNT(*)","SUM(?)","ROUND(AVG(?),2)"]'
  	)
  ) S
ON D.TARGET_TABLE = S.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
  TARGET_LABEL = S.TARGET_LABEL
  ,TARGET_TABLE = S.TARGET_TABLE
  ,BATCH_CONTROL_COLUMN = S.BATCH_CONTROL_COLUMN
  ,BATCH_CONTROL_SIZE = S.BATCH_CONTROL_SIZE
  ,BATCH_CONTROL_NEXT = S.BATCH_CONTROL_NEXT
  ,BATCH_PROCESSED = S.BATCH_PROCESSED
  ,BATCH_PROCESSING = S.BATCH_PROCESSING
  ,BATCH_MICROCHUNK_CURRENT = S.BATCH_MICROCHUNK_CURRENT
  ,BATCH_SCHEDULE_TYPE = S.BATCH_SCHEDULE_TYPE
  ,BATCH_SCHEDULE_LAST = S.BATCH_SCHEDULE_LAST
  ,PATTERN_COLUMNS = S.PATTERN_COLUMNS
  ,GROUPBY_COLUMNS = S.GROUPBY_COLUMNS
  ,GROUPBY_PATTERN = S.GROUPBY_PATTERN
  ,GROUPBY_FLEXIBLE = S.GROUPBY_FLEXIBLE
  ,AGGREGATE_COLUMNS = S.AGGREGATE_COLUMNS
  ,AGGREGATE_FUNCTIONS = S.AGGREGATE_FUNCTIONS
  ,DEFAULT_PROCEDURE = S.DEFAULT_PROCEDURE
WHEN NOT MATCHED THEN INSERT (
	TARGET_LABEL
	,TARGET_TABLE
	,BATCH_CONTROL_COLUMN
	,BATCH_CONTROL_SIZE
	,BATCH_CONTROL_NEXT
	,BATCH_PROCESSED
	,BATCH_PROCESSING
	,BATCH_MICROCHUNK_CURRENT
	,BATCH_SCHEDULE_TYPE
	,BATCH_SCHEDULE_LAST
	,PATTERN_COLUMNS
	,GROUPBY_COLUMNS
	,GROUPBY_PATTERN
	,GROUPBY_FLEXIBLE
	,AGGREGATE_COLUMNS
	,AGGREGATE_FUNCTIONS
	,DEFAULT_PROCEDURE
	)
VALUES (
  S.TARGET_LABEL
	,S.TARGET_TABLE
	,S.BATCH_CONTROL_COLUMN
	,S.BATCH_CONTROL_SIZE
	,S.BATCH_CONTROL_NEXT
	,S.BATCH_PROCESSED
	,S.BATCH_PROCESSING
	,S.BATCH_MICROCHUNK_CURRENT
	,S.BATCH_SCHEDULE_TYPE
	,S.BATCH_SCHEDULE_LAST
	,S.PATTERN_COLUMNS
	,S.GROUPBY_COLUMNS
	,S.GROUPBY_PATTERN
	,S.GROUPBY_FLEXIBLE
	,S.AGGREGATE_COLUMNS
	,S.AGGREGATE_FUNCTIONS
	,S.DEFAULT_PROCEDURE
);
```

#### 2. Aggregation Source Setup

```
--
-- Update or add the 1st aggregation source
--
MERGE INTO DATA_AGGREGATION_SOURCES D
USING (
  SELECT 'Test: dummy aggregation target 1 source 1' SOURCE_LABEL
    	,$1 TARGET_TABLE
    	,$2 SOURCE_TABLE
    	,true SOURCE_ENABLED
    	,0 PATTERN_DEFAULT
    	,False PATTERN_FLEXIBLE
    	,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE, '2000-01-01',TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()))/5)*5,'2000-01-01') DATA_AVAILABLETIME
    	,NULL DATA_CHECKSCHEDULE
    	,$3 TRANSFORMATION
  FROM VALUES (
		'_TEST_DATA_TARGET_1'
		,'_TEST_DATA_SOURCE_1'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_1\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_1
		'
  )
) S
ON D.TARGET_TABLE = S.TARGET_TABLE AND D.SOURCE_TABLE = S.SOURCE_TABLE
WHEN MATCHED THEN UPDATE SET ID = D.ID
	,SOURCE_LABEL = S.SOURCE_LABEL
	--,TARGET_TABLE = S.TARGET_TABLE
	--,SOURCE_TABLE = S.SOURCE_TABLE
	,SOURCE_ENABLED = S.SOURCE_ENABLED
	,PATTERN_DEFAULT = S.PATTERN_DEFAULT
	,PATTERN_FLEXIBLE = S.PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME = S.DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE = S.DATA_CHECKSCHEDULE
	,TRANSFORMATION = S.TRANSFORMATION
WHEN NOT MATCHED THEN INSERT (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
VALUES (
	S.SOURCE_LABEL
	,S.TARGET_TABLE
	,S.SOURCE_TABLE
	,S.SOURCE_ENABLED
	,S.PATTERN_DEFAULT
	,S.PATTERN_FLEXIBLE
	,S.DATA_AVAILABLETIME
	,S.DATA_CHECKSCHEDULE
	,S.TRANSFORMATION
	)
;

--
-- Update or add the 2nd aggregation source
--
MERGE INTO DATA_AGGREGATION_SOURCES D
USING (
  SELECT 'Test: Dummy aggregation target 1 source 2' SOURCE_LABEL
    	,$1 TARGET_TABLE
    	,$2 SOURCE_TABLE
    	,true SOURCE_ENABLED
    	,0 PATTERN_DEFAULT
    	,False PATTERN_FLEXIBLE
    	,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE, '2000-01-01',TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP()))/5)*5,'2000-01-01') DATA_AVAILABLETIME
    	,NULL DATA_CHECKSCHEDULE
    	,$3 TRANSFORMATION
  FROM VALUES (
		'_TEST_DATA_TARGET_1'
		,'_TEST_DATA_SOURCE_2'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_2\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_2
		'
  )
) S
ON D.TARGET_TABLE = S.TARGET_TABLE AND D.SOURCE_TABLE = S.SOURCE_TABLE
WHEN MATCHED THEN UPDATE SET ID = D.ID
	,SOURCE_LABEL = S.SOURCE_LABEL
	--,TARGET_TABLE = S.TARGET_TABLE
	--,SOURCE_TABLE = S.SOURCE_TABLE
	,SOURCE_ENABLED = S.SOURCE_ENABLED
	,PATTERN_DEFAULT = S.PATTERN_DEFAULT
	,PATTERN_FLEXIBLE = S.PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME = S.DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE = S.DATA_CHECKSCHEDULE
	,TRANSFORMATION = S.TRANSFORMATION
WHEN NOT MATCHED THEN INSERT (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
VALUES (
  S.SOURCE_LABEL
	,S.TARGET_TABLE
	,S.SOURCE_TABLE
	,S.SOURCE_ENABLED
	,S.PATTERN_DEFAULT
	,S.PATTERN_FLEXIBLE
	,S.DATA_AVAILABLETIME
	,S.DATA_CHECKSCHEDULE
	,S.TRANSFORMATION
	)
;
```
#### 3. Generate The Aggregation

```
--
-- Populate summary data for one day
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_1', '2020-01-07', 0);
--
-- Populate summary data over all available data
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_1', 0);
--
-- Check result of aggregation 1
--
select data_ts, count(*) cnt
from _TEST_DATA_TARGET_1
group by 1
order by 1 desc
;
```

### B. Demo-2: Aggregate data in daily based batches


The target table namely “_TEST_DATA_TARGET_2” is created by running the following SQL script, then we go through the listed steps in following sections to complete the full setup.

```
--
-- Create dummy aggregation data target 2
--
CREATE OR REPLACE TRANSIENT TABLE _TEST_DATA_TARGET_2 (
	"DATA_PT"	NUMBER,
	"DATA_DN"	VARCHAR,
	"DATA_DT"	DATE			NOT NULL,
	"DATA_HR"	NUMBER,
	"DATA_TS"	TIMESTAMP_NTZ,
	"DATA_I1"	NUMBER			NOT NULL,
	"DATA_I2"	NUMBER,
	"DATA_I3"	NUMBER,
	"DATA_A1"	VARCHAR,
	"DATA_A2"	VARCHAR,
	"DATA_A3"	VARCHAR,
	"VSUM_I1"	NUMBER,
	"VCNT_I2"	NUMBER,
	"VSUM_D1"	FLOAT,
	"VAVG_D2"	FLOAT
);
```

#### 1. Aggregation Target Setup

```
-- Update or add the aggregation target
MERGE INTO DATA_AGGREGATION_TARGETS D
USING (
  SELECT 'Test: Dummy aggregation target 2' TARGET_LABEL
  	,$1 TARGET_TABLE
  	,$2 BATCH_CONTROL_COLUMN
  	,$3 BATCH_CONTROL_SIZE
  	,$4 BATCH_CONTROL_NEXT
  	,DATE_TRUNC('DAY', CURRENT_DATE ()-7) BATCH_PROCESSED
  	,NULL BATCH_PROCESSING
  	,NULL BATCH_MICROCHUNK_CURRENT
  	,$5 BATCH_SCHEDULE_TYPE
  	,NULL BATCH_SCHEDULE_LAST
  	,PARSE_JSON($6) PATTERN_COLUMNS
  	,PARSE_JSON($7) GROUPBY_COLUMNS
  	,DATA_PATTERN(PARSE_JSON($8)) GROUPBY_PATTERN
  	,True GROUPBY_FLEXIBLE
  	,PARSE_JSON($9) AGGREGATE_COLUMNS
  	,PARSE_JSON($10) AGGREGATE_FUNCTIONS
  	,NULL DEFAULT_PROCEDURE
  FROM VALUES (
  	'_TEST_DATA_TARGET_2'
  	,'DATA_TS'
  	, 1440
  	,'DATEADD(MINUTE, :2, :1)'
  	,'DAILY'
  	-- all group-by columns in source data
  	,'["DATA_PATTERN",
  		"DATA_NAME",
      "DATA_DATE",
      "DATA_HOUR",
      "DATA_TIME",
  		"DATA_I1",
  		"DATA_I2",
  		"DATA_I3",
  		"DATA_A1",
  		"DATA_A2",
  		"DATA_A3"
        ]'
  	-- group-by columns of target data and which source column is the match
  	,'["DATA_DT:DATA_DATE",
  		"DATA_I1:DATA_I1",
  		"DATA_I2:DATA_I2",
  		"DATA_I3:DATA_I3",
  		"DATA_A1:DATA_A1",
  		"DATA_A2:DATA_A2",
  		"DATA_A3:DATA_A3"
         ]'
  	-- indicators of which group-by column are needed in target table
  	,'[0,0,1,0,0,1,1,1,1,1,1]'
  	-- aggregate columns of target data and which aggregating column is the match
  	,'["VSUM_I1:VALUE_I1","VCNT_I2:VALUE_I2","VSUM_D1:VALUE_D1","VAVG_D2:VALUE_D2"]'
  	-- what aggregation function will be used for every aggregation column
  	,'["SUM(?)","COUNT(DISTINCT ?)","SUM(?)","ROUND(AVG(?),2)"]'
  	)
  ) S
ON D.TARGET_TABLE = S.TARGET_TABLE
WHEN MATCHED THEN UPDATE SET
  TARGET_LABEL = S.TARGET_LABEL
  ,TARGET_TABLE = S.TARGET_TABLE
  ,BATCH_CONTROL_COLUMN = S.BATCH_CONTROL_COLUMN
  ,BATCH_CONTROL_SIZE = S.BATCH_CONTROL_SIZE
  ,BATCH_CONTROL_NEXT = S.BATCH_CONTROL_NEXT
  ,BATCH_PROCESSED = S.BATCH_PROCESSED
  ,BATCH_PROCESSING = S.BATCH_PROCESSING
  ,BATCH_MICROCHUNK_CURRENT = S.BATCH_MICROCHUNK_CURRENT
  ,BATCH_SCHEDULE_TYPE = S.BATCH_SCHEDULE_TYPE
  ,BATCH_SCHEDULE_LAST = S.BATCH_SCHEDULE_LAST
  ,PATTERN_COLUMNS = S.PATTERN_COLUMNS
  ,GROUPBY_COLUMNS = S.GROUPBY_COLUMNS
  ,GROUPBY_PATTERN = S.GROUPBY_PATTERN
  ,GROUPBY_FLEXIBLE = S.GROUPBY_FLEXIBLE
  ,AGGREGATE_COLUMNS = S.AGGREGATE_COLUMNS
  ,AGGREGATE_FUNCTIONS = S.AGGREGATE_FUNCTIONS
  ,DEFAULT_PROCEDURE = S.DEFAULT_PROCEDURE
WHEN NOT MATCHED THEN INSERT (
	TARGET_LABEL
	,TARGET_TABLE
	,BATCH_CONTROL_COLUMN
	,BATCH_CONTROL_SIZE
	,BATCH_CONTROL_NEXT
	,BATCH_PROCESSED
	,BATCH_PROCESSING
	,BATCH_MICROCHUNK_CURRENT
	,BATCH_SCHEDULE_TYPE
	,BATCH_SCHEDULE_LAST
	,PATTERN_COLUMNS
	,GROUPBY_COLUMNS
	,GROUPBY_PATTERN
	,GROUPBY_FLEXIBLE
	,AGGREGATE_COLUMNS
	,AGGREGATE_FUNCTIONS
	,DEFAULT_PROCEDURE
	)
VALUES (
  S.TARGET_LABEL
	,S.TARGET_TABLE
	,S.BATCH_CONTROL_COLUMN
	,S.BATCH_CONTROL_SIZE
	,S.BATCH_CONTROL_NEXT
	,S.BATCH_PROCESSED
	,S.BATCH_PROCESSING
	,S.BATCH_MICROCHUNK_CURRENT
	,S.BATCH_SCHEDULE_TYPE
	,S.BATCH_SCHEDULE_LAST
	,S.PATTERN_COLUMNS
	,S.GROUPBY_COLUMNS
	,S.GROUPBY_PATTERN
	,S.GROUPBY_FLEXIBLE
	,S.AGGREGATE_COLUMNS
	,S.AGGREGATE_FUNCTIONS
	,S.DEFAULT_PROCEDURE
);
```

#### 2. Aggregation Source Setup

```
--
-- Update or add the 1st aggregation source
--
MERGE INTO DATA_AGGREGATION_SOURCES D
USING (
  SELECT 'Test: Dummy aggregation target 2 source 1' SOURCE_LABEL
      ,$1 TARGET_TABLE
      ,$2 SOURCE_TABLE
      ,true SOURCE_ENABLED
      ,0 PATTERN_DEFAULT
      ,False PATTERN_FLEXIBLE
      ,DATE_TRUNC('DAY', CURRENT_DATE()) -1 DATA_AVAILABLETIME
      ,NULL DATA_CHECKSCHEDULE
      ,$3 TRANSFORMATION
  FROM VALUES (
		'_TEST_DATA_TARGET_2'
		,'_TEST_DATA_SOURCE_1'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_1\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_1
		'
  )
) S
ON D.TARGET_TABLE = S.TARGET_TABLE AND D.SOURCE_TABLE = S.SOURCE_TABLE
WHEN MATCHED THEN UPDATE SET ID = D.ID
	,SOURCE_LABEL = S.SOURCE_LABEL
	--,TARGET_TABLE = S.TARGET_TABLE
	--,SOURCE_TABLE = S.SOURCE_TABLE
	,SOURCE_ENABLED = S.SOURCE_ENABLED
	,PATTERN_DEFAULT = S.PATTERN_DEFAULT
	,PATTERN_FLEXIBLE = S.PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME = S.DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE = S.DATA_CHECKSCHEDULE
	,TRANSFORMATION = S.TRANSFORMATION
WHEN NOT MATCHED THEN INSERT (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
VALUES (
  S.SOURCE_LABEL
	,S.TARGET_TABLE
	,S.SOURCE_TABLE
	,S.SOURCE_ENABLED
	,S.PATTERN_DEFAULT
	,S.PATTERN_FLEXIBLE
	,S.DATA_AVAILABLETIME
	,S.DATA_CHECKSCHEDULE
	,S.TRANSFORMATION
	)
;

--
-- Update or add the 2nd aggregation source
--
MERGE INTO DATA_AGGREGATION_SOURCES D
USING (
  SELECT 'Test: Dummy aggregation target 2 source 2' SOURCE_LABEL
      ,$1 TARGET_TABLE
      ,$2 SOURCE_TABLE
      ,true SOURCE_ENABLED
      ,0 PATTERN_DEFAULT
      ,False PATTERN_FLEXIBLE
      ,DATE_TRUNC('DAY', CURRENT_DATE()) -1 DATA_AVAILABLETIME
      ,NULL DATA_CHECKSCHEDULE
      ,$3 TRANSFORMATION
  FROM VALUES (
		'_TEST_DATA_TARGET_2'
		,'_TEST_DATA_SOURCE_2'
		,'
		SELECT DATA_PT DATA_PATTERN
			,\'_TEST_DATA_SOURCE_2\'::VARCHAR DATA_NAME
			,DATE(DATA_TS) DATA_DATE
      ,DATE_PART(HOUR, DATA_TS) DATA_HOUR
      ,DATEADD(MINUTE, ROUND(DATEDIFF(MINUTE,\'2000-01-01\',DATA_TS)/5)*5,\'2000-01-01\') DATA_TIME
			,DATA_TS
			,DATA_I1
			,DATA_I2
			,DATA_I3
			,DATA_A1
			,DATA_A2
			,DATA_A3
			,VALUE_I1
			,VALUE_I1 VALUE_I2
			,VALUE_D1
			,VALUE_D1 VALUE_D2
		FROM _TEST_DATA_SOURCE_2
		'
	)
) S
ON D.TARGET_TABLE = S.TARGET_TABLE AND D.SOURCE_TABLE = S.SOURCE_TABLE
WHEN MATCHED THEN UPDATE SET ID = D.ID
	,SOURCE_LABEL = S.SOURCE_LABEL
	--,TARGET_TABLE = S.TARGET_TABLE
	--,SOURCE_TABLE = S.SOURCE_TABLE
	,SOURCE_ENABLED = S.SOURCE_ENABLED
	,PATTERN_DEFAULT = S.PATTERN_DEFAULT
	,PATTERN_FLEXIBLE = S.PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME = S.DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE = S.DATA_CHECKSCHEDULE
	,TRANSFORMATION = S.TRANSFORMATION
WHEN NOT MATCHED THEN INSERT (
	SOURCE_LABEL
	,TARGET_TABLE
	,SOURCE_TABLE
	,SOURCE_ENABLED
	,PATTERN_DEFAULT
	,PATTERN_FLEXIBLE
	,DATA_AVAILABLETIME
	,DATA_CHECKSCHEDULE
	,TRANSFORMATION
	)
VALUES (
  S.SOURCE_LABEL
	,S.TARGET_TABLE
	,S.SOURCE_TABLE
	,S.SOURCE_ENABLED
	,S.PATTERN_DEFAULT
	,S.PATTERN_FLEXIBLE
	,S.DATA_AVAILABLETIME
	,S.DATA_CHECKSCHEDULE
	,S.TRANSFORMATION
	)
;
```
#### 3. Generate The Aggregation

```
--
-- Populate summary data of one day
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_2', '2020-01-07', 0);
--
-- Populate summary data of all available dayes
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_2', 0);
--
-- Check result of aggregation 2
--
select data_ts, count(*) cnt
from _TEST_DATA_TARGET_2
group by 1
order by 1 desc
;
```

## V. Schedule The Process


### A. Kick a Manual Run

#### 1. Run a Single chunk process

```
--
-- Manually test one day's data population with demo-1 settings
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_1', '2020-01-07', 0);

--
-- Manually test one day's data population with demo-2 settings
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_2', '2020-01-07', 0);
```


#### 2. Run a batch period process

```
--
-- Setup an initial starting date for demo-1
--
UPDATE BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGETS
SET BATCH_PROCESSED = '2020-05-31'
	,BATCH_PROCESSING = NULL
WHERE TARGET_TABLE = '_TEST_DATA_TARGET_1';

--
-- Automate the Aggregate data population for demo-1
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_1', 0);



--
-- Setup an initial starting date for demo-2
--
UPDATE BI._CONTROL_LOGIC.DATA_AGGREGATION_TARGETS
SET BATCH_PROCESSED = '2020-05-31'
	,BATCH_PROCESSING = NULL
WHERE TARGET_TABLE = '_TEST_DATA_TARGET_2';

--
-- Automate the Aggregate data population for demo-2
--
CALL DATA_AGGREGATOR('_TEST_DATA_TARGET_2', 0);

```

### B. Schedule by Tasks

#### 1. Setup Fully Automated Tasks

```
-- Create root task with a schedule
CREATE OR REPLACE TASK HOURLY_SOURCE_DATA_AVAILABILITY_1
    WAREHOUSE = S1_BI
    SCHEDULE = 'USING CRON 0 9-18 * * * America/Los_Angeles'
AS
( Call a procedure to refresh the available time of all sources here. This SP does not exist yet.);

-- Create follower task with "after" cause
CREATE  OR REPLACE TASK HOURLY_POPULATE_AGGREGATION_FOR_DEMO_1
  WAREHOUSE = S1_BI
  AFTER HOURLY_SOURCE_DATA_AVAILABILITY_1
AS
CALL DATA_AGGREGATOR ('_TEST_DATA_TARGET_1', 0);

-- Enable the root task for hourly scheduled
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('HOURLY_SOURCE_DATA_AVAILABILITY_1');
```


#### 2. Setup Range Sliding Type Tasks

```
-- Create root task with a schedule
CREATE OR REPLACE TASK HOURLY_SOURCE_DATA_AVAILABILITY_2
    WAREHOUSE = S1_BI
    SCHEDULE = 'USING CRON 0 9-18 * * * America/Los_Angeles'
AS
UPDATE DATA_AGGREGATION_SOURCES
SET DATA_AVAILABLETIME = DATEADD(DAY, -60, CURRENT_DATE())
WHERE TARGET_TABLE = '_TEST_DATA_TARGET_2';

-- Create follower task with "after" cause
CREATE  OR REPLACE TASK HOURLY_POPULATE_AGGREGATION_FOR_DEMO_2
  WAREHOUSE = S1_BI
  AFTER HOURLY_SOURCE_DATA_AVAILABILITY_2
AS
CALL DATA_AGGREGATOR ('_TEST_DATA_TARGET_2', 0);

-- Enable the root task for hourly scheduled
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('HOURLY_SOURCE_DATA_AVAILABILITY_2');
```

## VI. Author



## VII. License



## VIII. Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc
