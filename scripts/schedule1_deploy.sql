!set variable_substitution=true;
use database &{db_name};
create schema if not exists &{sc_name};
--create schema &{sc_name};
use schema &{sc_name};
--
-------------------------------------------------------
-- Create task management tables
-------------------------------------------------------
--
-- DROP SEQUENCE DATA_AGGREGATION_SOURCES_SEQ;;
--
CREATE SEQUENCE DATA_AGGREGATION_TARGETS_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_AGGREGATION_TARGETS;
--
CREATE TABLE DATA_AGGREGATION_TARGETS
(
	ID 												NUMBER NOT NULL DEFAULT DATA_AGGREGATION_TARGETS_SEQ.NEXTVAL,
	TARGET_LABEL							TEXT,
	TARGET_TABLE							TEXT NOT NULL,
	BATCH_CONTROL_COLUMN			TEXT,
	BATCH_CONTROL_SIZE				NUMBER,
	BATCH_CONTROL_NEXT				TEXT,
	BATCH_PROCESSED		    		TIMESTAMP_NTZ,
	BATCH_PROCESSING					TIMESTAMP_NTZ,
	BATCH_MICROCHUNK_CURRENT 	TIMESTAMP_NTZ,
	BATCH_SCHEDULE_TYPE				TEXT,
	BATCH_SCHEDULE_LAST				TIMESTAMP_NTZ,
	PATTERN_COLUMNS		    		ARRAY,
	GROUPBY_COLUMNS		    		ARRAY,
	GROUPBY_PATTERN		    		NUMBER,
	GROUPBY_FLEXIBLE					BOOLEAN,
	AGGREGATE_COLUMNS					ARRAY,
	AGGREGATE_FUNCTIONS				ARRAY,
	DEFAULT_PROCEDURE					TEXT,
	CONSTRAINT PK_DATA_AGGREGATION_TARGETS PRIMARY KEY (TARGET_TABLE)
)
CLUSTER BY (TARGET_TABLE)
COMMENT = 'This tableis used to register the aggregation targets'
;
--
-- DROP SEQUENCE DATA_AGGREGATION_SOURCES_SEQ;;
--
CREATE SEQUENCE DATA_AGGREGATION_SOURCES_SEQ START = 1 INCREMENT = 1;
--
-- DROP TABLE DATA_AGGREGATION_SOURCES;
--
CREATE TABLE DATA_AGGREGATION_SOURCES
(
	ID 												NUMBER NOT NULL DEFAULT DATA_AGGREGATION_SOURCES_SEQ.NEXTVAL,
	SOURCE_LABEL							TEXT,
	TARGET_TABLE	        		TEXT NOT NULL,
	SOURCE_TABLE	        		TEXT NOT NULL,
	SOURCE_ENABLED	        	BOOLEAN,
	PATTERN_DEFAULT	        	NUMBER,
	PATTERN_FLEXIBLE	    		BOOLEAN,
	DATA_AVAILABLETIME	    	TIMESTAMP_NTZ,
	DATA_CHECKSCHEDULE	    	TIMESTAMP_NTZ,
	TRANSFORMATION	        	TEXT,
	CONSTRAINT PK_DATA_AGGREGATION_SOURCES PRIMARY KEY (TARGET_TABLE, SOURCE_TABLE),
	CONSTRAINT FK_DATA_AGGREGATION_SOURCES_TARGET_TABLE FOREIGN KEY (TARGET_TABLE)
		REFERENCES DATA_AGGREGATION_TARGETS(TARGET_TABLE)
)
CLUSTER BY (TARGET_TABLE, SOURCE_TABLE)
COMMENT = 'This tableis used to register the aggregation sources'
;
--
-------------------------------------------------------
-- Create assisstant functions
-------------------------------------------------------
--
-- DROP FUNCTION DATA_PATTERN(ARRAY);
--
CREATE FUNCTION DATA_PATTERN(
	P ARRAY
	)
RETURNS DOUBLE
LANGUAGE JAVASCRIPT
AS
$$
if (typeof P !== "undefined" || P !== null) {
  datPat = 0, misBit = 0;
  if (typeof P[0] === "object") {
    Q = P[0]["pattern_columns"];
    R = P[0]["groupby_columns"];
    if (typeof Q !== "undefined" || Q !== null || typeof R !== "undefined" || R !== null) {
      patLen = Q.length;
      for (i = 0; i < patLen; i++) {
          if (R.indexOf(Q[i]) !== -1) {
              misBit = 0;
          } else {
              misBit = 1;
          }
          datPat = 2 * datPat + misBit;
      }
    }
  } else {
    patLen = P.length;
    for (i = 0; i < patLen; i++) {
      if (P[i]) {
          misBit = 0;
      } else {
          misBit = 1;
      }
      datPat = 2 * datPat + misBit;
    }
  }
}
return datPat;
$$;
--
--
-- DROP FUNCTION COLUMN_MAP(ARRAY);
--
CREATE FUNCTION COLUMN_MAP(
	P ARRAY
	)
RETURNS VARIANT
LANGUAGE JAVASCRIPT
AS
$$
mapping = {};
if (P !== "undefined" || P !== null) {
  Q = P[0];
  if (Q["target_column_list"] !== "undefined"  && typeof Q["target_column_list"] === "object"
    && Q["source_column_list"] !== "undefined"  && typeof Q["source_column_list"] === "object"
    && Q["target_column_list"].length === Q["source_column_list"].length
  ) {
    patLen = Q["target_column_list"].length;
    for (i = 0; i < patLen; i++) {
        mapping[Q["target_column_list"][i]] = Q["source_column_list"][i];
    }
 }
}
return mapping;
$$;
--
--
-- DROP FUNCTION REVENUE_SHARE(VARIANT, VARCHAR, FLOAT);
--
CREATE FUNCTION REVENUE_SHARE(
	P VARIANT,
	D VARCHAR,
	V FLOAT
	)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
var rev_share = null, Q = D;
if (D) {Q = D.toUpperCase();}
if (typeof P === "undefined") {
    rev_share = null;
}
else if (typeof P === "number") {
    rev_share = P;
}
else if (typeof P[0] === "object" && typeof V !== "undefined") {
    rev_share = P.filter(x => (x["RANGE_LOWER"] <= V && (!x["RANGE_UPPER"] || x["RANGE_UPPER"] > V)))[0]["REVENUE_SHARE"];
}
else if (typeof P === "object") {
    if (typeof P[Q] === "number") {
        rev_share = P[Q];
    }
    else if (typeof P[Q] === "undefined" && typeof P["(Others)"] === "undefined") {
        rev_share = P["(OTHERS)"];
    }
}
return rev_share;
$$;
--
--
-- DROP FUNCTION REVENUE_SHARE(VARIANT, VARCHAR);
--
CREATE FUNCTION REVENUE_SHARE(
	P VARIANT,
	D VARCHAR
	)
RETURNS FLOAT
LANGUAGE JAVASCRIPT
AS
$$
var rev_share = null, Q = D;
if (D) {Q = D.toLowerCase(); if (["phone","smart phones"].includes(Q)) {Q = 'mobile'}}
if (typeof P === "undefined") {
    rev_share = null;
}
else if (typeof P === "number") {
    rev_share = P;
}
else if (typeof P === "object") {
  for (item in P) {
    if (P[item][Q] && typeof P[item][Q][0] === "number") {
        rev_share = P[item][Q][0];
        break;
    }
  }
  if (!rev_share) {
    for (item in P) {
      if (P[item]["other"] && typeof P[item]["other"][0] === "number") {
          rev_share = P[item]["other"][0];
          break;
      }
    }
  }
}
return rev_share;
$$;

-------------------------------------------------------
-- Create aggregator stored procedures
-------------------------------------------------------
--
-- Aggregate generation stored procedues for indivual source
-- DROP PROCEDURE DATA_AGGREGATOR(STRING, STRING, BOOLEAN);
--
CREATE PROCEDURE  DATA_AGGREGATOR (
	TARGET_TABLE STRING,
	BATCH_TIMETAG STRING,
	SCRIPT_ONLY BOOLEAN
	)
RETURNS STRING
LANGUAGE JAVASCRIPT STRICT
AS
$$
try {
	var sqlScript = '', sourceTitle = '';

	var sourceQuery = `SELECT
		  d.TARGET_TABLE,
		  d.BATCH_CONTROL_COLUMN,
		  d.BATCH_CONTROL_SIZE,
		  d.BATCH_CONTROL_NEXT,
		  d.PATTERN_COLUMNS,
		  d.GROUPBY_COLUMNS,
		  BITOR(d.GROUPBY_PATTERN, s.PATTERN_DEFAULT) GROUPBY_PATTERN,
		  d.GROUPBY_FLEXIBLE OR (d.GROUPBY_PATTERN = BITOR(d.GROUPBY_PATTERN, s.PATTERN_DEFAULT)) GROUPBY_COMPITABLE,
		  d.AGGREGATE_COLUMNS,
		  d.AGGREGATE_FUNCTIONS,
		  d.DEFAULT_PROCEDURE,
		  s.SOURCE_TABLE,
		  s.TRANSFORMATION
	  FROM DATA_AGGREGATION_TARGETS d
	  JOIN DATA_AGGREGATION_SOURCES s
	  USING(TARGET_TABLE)
	  WHERE s.SOURCE_ENABLED = True
		AND d.TARGET_TABLE = :1;`;

	var sourceStmt = snowflake.createStatement({
	  sqlText: sourceQuery,
	  binds: [TARGET_TABLE]
	  });

	var sources = sourceStmt.execute();

	// for each source
	while (sources.next())
	{
	  var targetTable = sources.getColumnValue(1);
	  var batchControlColumn = sources.getColumnValue(2);
	  var batchControlSize = sources.getColumnValue(3);
	  var batchControlNext = sources.getColumnValue(4);
	  var patternColumns = sources.getColumnValue(5);
	  var groupByColumns = sources.getColumnValue(6).map(x => x.split(':')[1]);
	  var dimensionColumns = sources.getColumnValue(6).map(x => x.split(':')[0]);
	  var groupByPattern = sources.getColumnValue(7);
	  var groupByCompitable = sources.getColumnValue(8);
	  var aggregateColumns = sources.getColumnValue(9).map(x => x.split(':')[1]);
	  var measureColumns = sources.getColumnValue(9).map(x => x.split(':')[0]);
	  var aggregateFunctions = sources.getColumnValue(10);
	  var defaultProcedure = sources.getColumnValue(11);
	  var sourceTable = sources.getColumnValue(12);
	  var transformation = sources.getColumnValue(13);
	  var sqlExecuted = '';

	  if (transformation) {transformation = '(' + transformation + ')'} else {transformation = sourceTable}

	  if (groupByCompitable) {
		var flagIndexLast = patternColumns.length - 1,
			patternSegment = groupByPattern;
		var selectList = groupByColumns[0] === "DATA_PATTERN" ? 'BITOR(' + groupByColumns[0] + ',' + groupByPattern + ') ' : '',
			dimensionList = '',
			groupByList = '',
			columnSplitter = '';
		for (var i = 0; i <= flagIndexLast; i++) {
		  var flagPower = 2 ** (flagIndexLast - i);
		  if (patternSegment / flagPower < 1) {
			dimensionList = dimensionList + columnSplitter + dimensionColumns[groupByColumns.indexOf(patternColumns[i])];
			selectList = selectList + columnSplitter + patternColumns[i];
			groupByList = groupByList + columnSplitter + patternColumns[i];
			columnSplitter = ',';
		  }
		  patternSegment %= flagPower;
		}

		var targetAlias = 'T.', sourceAlias = 'S.';
		var loadQuery = `MERGE INTO ` + targetTable + ` ` + targetAlias[0] + ` \n`
			+ `USING ( \n`
			+ `  SELECT ` + groupByList + `,`
					+ aggregateFunctions.map((x,i)=>{return x.replace('?', aggregateColumns[i]) + ' ' + aggregateColumns[i]}) + ` \n`
			+ `  FROM ( \n`
			+ `    SELECT ` + selectList + `,` + aggregateColumns + ` \n`
			+ `    FROM ` + transformation + ` \n`
			+ `    WHERE ` + batchControlColumn + ` >= :1 AND ` + batchControlColumn + ` < ` + batchControlNext + ` \n`
			+ `    ) \n`
			+ `  GROUP BY ` + groupByList + `\n`
			+ `  ) ` + sourceAlias[0] + ` \n`
			+ `ON ` + dimensionList.split(',').map((x,i)=>{return `COALESCE(TO_CHAR(` + targetAlias + x + `),'') = COALESCE(TO_CHAR(` + sourceAlias + groupByColumns[i] + `),'')`}).join('\n AND ') + ` \n`
			+ `WHEN MATCHED THEN UPDATE SET ` + measureColumns.map((x,i) =>{return x + ' = ' + sourceAlias[0] + `.` + aggregateColumns[i]}) + ` \n`
			+ `WHEN NOT MATCHED THEN INSERT(` + dimensionList + `,` + measureColumns + `) \n`
			+ `VALUES (` + groupByList.split(',').map(x=>{return sourceAlias[0] + `.` +  x}) + `,`
						 + aggregateColumns.map(x=>{return sourceAlias[0] + `.` +  x}) + `);`;


		var loadStmt = snowflake.createStatement({
			sqlText: loadQuery,
			binds: [BATCH_TIMETAG, batchControlSize]
			});

		if (!SCRIPT_ONLY) {loadStmt.execute();}

		sqlExecuted = loadStmt.getSqlText().replace(/:1/g, "'" + BATCH_TIMETAG + "'").replace(/:2/g, batchControlSize);
	  }
	  else {
		sqlExecuted = '-- No data is loaded from this source as a data pattern compatible issue!';
	  }

	  sourceTitle = `\n\n` + '-'.repeat(65)
		+ `\n-- ` + sourceTable.replace('DATAMART.BUYSIDE_NETWORK.','').replace('DATAMART.SELLSIDE_NETWORK.','')
		+ `\n` + '-'.repeat(65) + `\n`;
	  sqlScript = sqlScript + sourceTitle + sqlExecuted;
	}

	return sqlScript;
}
catch (err) {
	return "Failed: " + err
}
$$;
--
-- Aggregate stored procedues to loop all available source tables
-- DROP PROCEDURE DATA_AGGREGATOR(STRING, BOOLEAN);
--
CREATE PROCEDURE DATA_AGGREGATOR (
	TARGET_TABLE STRING,
	SCRIPT_ONLY BOOLEAN
	)
RETURNS STRING
LANGUAGE JAVASCRIPT STRICT
AS
$$
try {
	var batchControlColumn  = '',
		batchControlSize = 0,
		batchControlType  = '',
		batchLoopTag = '',
		batchLoopEnd = '',
		batchScheduleCurrent;
	var loopScript = '',
		loopSegmenter = '';

	//
	// Detect runable or not
	//
	var targetQuery = `SELECT BATCH_CONTROL_COLUMN,
	  BATCH_CONTROL_SIZE,
	  BATCH_SCHEDULE_TYPE,
	  --DATEADD(MINUTE, BATCH_CONTROL_SIZE, BATCH_PROCESSED) BATCH_LOOP_BEGIN,
	  --DATEADD(MINUTE, -BATCH_CONTROL_SIZE, BATCH_POSSIBLE) BATCH_LOOP_END,
	  DATEADD(MINUTE, CASE BATCH_SCHEDULE_TYPE
          WHEN 'MINUTES' THEN BATCH_CONTROL_SIZE
          WHEN 'HOURLY' THEN 60
          ELSE 1440
        END, BATCH_PROCESSED) BATCH_LOOP_BEGIN,
	  BATCH_POSSIBLE BATCH_LOOP_END,
	  BATCH_SCHEDULE_CURRENT
	FROM (
	  SELECT BATCH_CONTROL_COLUMN,
		  BATCH_CONTROL_SIZE,
		  BATCH_CONTROL_NEXT,
		  BATCH_PROCESSED,
		  BATCH_PROCESSING,
		  BATCH_SCHEDULE_TYPE,
		  BATCH_SCHEDULE_LAST,
		  CURRENT_TIMESTAMP() BATCH_SCHEDULE_CURRENT,
		  CASE BATCH_SCHEDULE_TYPE
			WHEN 'HOURLY' THEN DATE_TRUNC(HOUR, BATCH_SCHEDULE_CURRENT)
			WHEN 'DAILY' THEN DATE_TRUNC(DAY, BATCH_SCHEDULE_CURRENT)
			ELSE DATEADD(MINUTE,FLOOR(DATEDIFF(MINUTE,'1970-01-01',BATCH_SCHEDULE_CURRENT)/BATCH_CONTROL_SIZE)*BATCH_CONTROL_SIZE,'1970-01-01')
		  END BATCH_POSSIBLE
	  FROM (
		SELECT BATCH_CONTROL_COLUMN,
			BATCH_CONTROL_SIZE,
			BATCH_CONTROL_NEXT,
			BATCH_PROCESSED,
			BATCH_PROCESSING,
			BATCH_SCHEDULE_TYPE,
			BATCH_SCHEDULE_LAST,
			CURRENT_TIMESTAMP() BATCH_SCHEDULE_CURRENT
		FROM DATA_AGGREGATION_TARGETS
		WHERE TARGET_TABLE = :1
		)
	  )
	WHERE BATCH_PROCESSING IS NULL
	OR DATEDIFF(MINUTE, BATCH_SCHEDULE_LAST, BATCH_POSSIBLE) > BATCH_CONTROL_SIZE;`;

	var targetStmt = snowflake.createStatement({
	  sqlText: targetQuery,
	  binds: [TARGET_TABLE]
	  });

	var target = targetStmt.execute();

	if (target.next()) {
		batchControlColumn = target.getColumnValue(1);
		batchControlSize = target.getColumnValue(2);
		batchScheduleType = target.getColumnValue(3);
		batchLoopTag = target.getColumnValue(4);
		batchLoopEnd = target.getColumnValue(5);
		batchScheduleCurrent = target.getColumnValue(6);
	}
	else {
		return '\n\n-- Skip this schedule as previous schedule has not done yet!\n'
	}

	//
	// Initialize the batch exclusion control context
	//
	var contextQuery = `UPDATE DATA_AGGREGATION_TARGETS \n `
		+ `SET BATCH_PROCESSING = :2, \n\t `
		+ `BATCH_SCHEDULE_LAST = :3 \n`
		+ `WHERE TARGET_TABLE = :1;`;
	var contextStmt = snowflake.createStatement({
	  sqlText: contextQuery,
	  binds: [TARGET_TABLE, batchLoopEnd, batchScheduleCurrent]
	  });

	if (!SCRIPT_ONLY) {contextStmt.execute();}


	//
	// Loop and call the date_poplate SP for each batch
	//
	while (batchLoopTag <= batchLoopEnd)
	{
	  var contextQuery = `UPDATE DATA_AGGREGATION_TARGETS \n `
		  + `SET BATCH_MICROCHUNK_CURRENT = :2 \n `
		  + `WHERE TARGET_TABLE = :1;`;
	  var contextStmt = snowflake.createStatement({
		sqlText: contextQuery,
		binds: [TARGET_TABLE, batchLoopTag.toISOString()]
		});
	  if (!SCRIPT_ONLY) {contextStmt.execute();}

	  var removalQuery = `DELETE FROM ` + TARGET_TABLE
		  + ` WHERE ` + batchControlColumn + ` >= :1`
		  + ` AND ` + batchControlColumn + ` < DATEADD(MINUTE, :2, :1);\n`;
	  var removalStmt = snowflake.createStatement({
		  sqlText: removalQuery,
		  binds: [batchLoopTag.toISOString(), batchControlSize]
		  });
	  if (!SCRIPT_ONLY) {removalStmt.execute();}

	  var callQuery = 'CALL DATA_AGGREGATOR (:1, :2, :3);';
	  var callStmt = snowflake.createStatement({
		sqlText: callQuery,
		binds: [TARGET_TABLE, batchLoopTag.toISOString(), SCRIPT_ONLY]
		});
	  if (!SCRIPT_ONLY) {callStmt.execute();}

	  loopSegmenter = `\n\n--` + '='.repeat(65)
		+ `\n-- ` + batchLoopTag.toISOString()
		+ `\n--` + '='.repeat(65) + `\n`;
	  loopScript = loopScript + loopSegmenter
		+ removalStmt.getSqlText()
			.replace(/:1/g, '\'' + batchLoopTag.toISOString() + '\'')
			.replace(/:2/g, '\'' + batchControlSize + '\'')
		+ callStmt.getSqlText()
			.replace(/:1/g, '\'' + TARGET_TABLE + '\'')
			.replace(/:2/g, '\'' + batchLoopTag.toISOString() + '\'')
			.replace(/:3/g, + SCRIPT_ONLY.toString());

	  batchLoopTag.setMinutes(batchLoopTag.getMinutes() + batchControlSize);
	}

	//
	// Clear the batch exclusion control context
	//
	var contextQuery = `UPDATE DATA_AGGREGATION_TARGETS T \n`
		+ `SET BATCH_MICROCHUNK_CURRENT = NULL, BATCH_PROCESSING = NULL, BATCH_PROCESSED = S.DATA_AVAILABLETIME \n`
		+ `FROM ( \n`
		  + `SELECT d.TARGET_TABLE, MIN(COALESCE(s.DATA_AVAILABLETIME, d.BATCH_PROCESSED)) DATA_AVAILABLETIME \n`
		  + `FROM DATA_AGGREGATION_TARGETS d \n`
		  + `JOIN DATA_AGGREGATION_SOURCES s \n`
		  + `USING(TARGET_TABLE) \n`
		  + `WHERE s.SOURCE_ENABLED = True \n`
		  + `GROUP BY d.TARGET_TABLE \n`
		  + `) S \n`
		+ `WHERE T.TARGET_TABLE = S.TARGET_TABLE AND T.TARGET_TABLE = :1;`;
	var contextStmt = snowflake.createStatement({
	  sqlText: contextQuery,
	  binds: [TARGET_TABLE]
	  });

	if (!SCRIPT_ONLY) {contextStmt.execute();}

	return loopScript;
}
catch (err) {
	return "Failed: " + err
}
$$;
