package actions

import (
	"fmt"
	"strings"

	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
)

type DemoSetupConfig struct {
	LogLevel         string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic bool
	ExecuteDDL       bool
	OraSchemaTable   rdbms.SchemaTable // reused for Snowflake table.
	SourceString     ConnectionObject
	TargetString     ConnectionObject
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	OraConnDetails   *shared.DsnConnectionDetails
	SnowConnDetails  *shared.DsnConnectionDetails
	SnowStageName    string `errorTxt:"Snowflake stage name" mandatory:"yes"`
	SnowS3Url        string `errorTxt:"Snowflake stage S3 URL" mandatory:"yes"`
	SnowS3Key        string `errorTxt:"S3 access key for Snowflake stage" mandatory:"yes"`
	SnowS3Secret     string `errorTxt:"S3 secret key for Snowflake stage" mandatory:"yes"`
}

type DemoCleanupConfig struct {
	LogLevel         string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic bool
	ExecuteDDL       bool
	SourceString     ConnectionObject
	TargetString     ConnectionObject
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	OraConnDetails   *shared.DsnConnectionDetails
	SnowConnDetails  *shared.DsnConnectionDetails
	OraSchemaTable   rdbms.SchemaTable // reused for Snowflake table.
	SnowStageName    string            `errorTxt:"Snowflake stage name" mandatory:"yes"`
}

func RunDemoFullSetup(cfg *DemoSetupConfig) error {
	// Setup logging.
	if cfg.LogLevel == "" {
		cfg.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfg.LogLevel, cfg.StackDumpOnPanic)
	// Get specific connections.
	cfg.OraConnDetails = shared.GetDsnConnectionDetails(cfg.SrcConnDetails)
	cfg.SnowConnDetails = shared.GetDsnConnectionDetails(cfg.TgtConnDetails)
	err := validateDemoFullSetupConfig(cfg)
	if err != nil {
		return err
	}
	printLogFn := getPrintLogFunc(log, !cfg.ExecuteDDL)
	// Get Oracle Table DDL and DML for sample data.
	oraDDL := getOracleDemoTableDDL(cfg.OraSchemaTable.SchemaTable, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	printLogFn(`-- Oracle SQL...`)
	for _, stmt := range oraDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.OracleExecWithConnDetails(log, cfg.OraConnDetails, stmt)
			}
			mustExecFn(log, printLogFn, fn)
		}
	}
	// Get Snowflake table DDL.
	snowDDL := getSnowflakeDemoTableDDL(cfg.OraSchemaTable.SchemaTable, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	printLogFn(`-- Snowflake SQL...`)
	for _, stmt := range snowDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.SnowflakeDDLExec(log, cfg.SnowConnDetails, stmt)
			}
			mustExecFn(log, printLogFn, fn)
		}
	}
	// Get Snowflake Stage DDL.
	stageDDL := getSnowflakeStageDDL(cfg.SnowStageName, cfg.SnowS3Url, cfg.SnowS3Key, cfg.SnowS3Secret, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	for _, stmt := range stageDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.SnowflakeDDLExec(log, cfg.SnowConnDetails, stmt)
			}
			mustExecFn(log, printLogFn, fn)
		}
	}
	return err
}

func validateDemoFullSetupConfig(cfg *DemoSetupConfig) (err error) {
	errs := make([]string, 0)
	helper.GetStructErrorTxt4UnsetFields(cfg, &errs)
	if len(errs) > 0 {
		err = fmt.Errorf("please supply values for %v", strings.Join(errs, ", "))
	}
	return
}

func RunDemoCleanup(cfg *DemoCleanupConfig) error {
	// Setup logging.
	if cfg.LogLevel == "" {
		cfg.LogLevel = "error"
	}
	log := logger.NewLogger("halfpipe", cfg.LogLevel, true)
	// Get specific connections.
	cfg.OraConnDetails = shared.GetDsnConnectionDetails(cfg.SrcConnDetails)
	cfg.SnowConnDetails = shared.GetDsnConnectionDetails(cfg.TgtConnDetails)
	err := validateCleanupConfig(cfg)
	if err != nil {
		return err
	}
	printLogFn := getPrintLogFunc(log, !cfg.ExecuteDDL)
	// Get Oracle Table DDL and DML for sample data.
	oraDDL := getOracleDemoTableCleanupDDL(cfg.OraSchemaTable.SchemaTable, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	printLogFn(`-- Oracle SQL...`)
	for _, stmt := range oraDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.OracleExecWithConnDetails(log, cfg.OraConnDetails, stmt)
			}
			mustExecFn(log, printLogFn, fn)
		}
	}
	// Get Snowflake table DDL.
	snowDDL := getSnowflakeDemoTableCleanupDDL(cfg.OraSchemaTable.SchemaTable, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	printLogFn(`-- Snowflake SQL...`)
	for _, stmt := range snowDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.SnowflakeDDLExec(log, cfg.SnowConnDetails, stmt)
			}
			mustExecFn(log, printLogFn, fn)
		}
	}
	// Get Snowflake Stage DDL.
	stageDDL := getSnowflakeStageCleanupDDL(cfg.SnowStageName, !cfg.ExecuteDDL) // if we want to execute then disable terminator in SQL strings.
	for _, stmt := range stageDDL {
		printLogFn(stmt)
		if cfg.ExecuteDDL {
			fn := func() error {
				return rdbms.SnowflakeDDLExec(log, cfg.SnowConnDetails, stmt)
			}
			mustExecFn(log, printLogFn, fn)
		}
	}
	return nil
}

func validateCleanupConfig(cfg *DemoCleanupConfig) (err error) {
	errs := make([]string, 0)
	helper.GetStructErrorTxt4UnsetFields(cfg, &errs)
	if len(errs) > 0 {
		err = fmt.Errorf("please supply values for %v", strings.Join(errs, ", "))
	}
	return
}

func getOracleDemoTableDDL(tableName string, addTerminator bool) []string {
	s := make([]string, 0, 5)
	terminator := ""
	if addTerminator {
		terminator = ";"
	}
	s = append(s, fmt.Sprintf(`begin execute immediate 'drop table %v purge'; exception when others then null; end;`, tableName))
	if addTerminator {
		s[0] = s[0] + " /"
	}
	s = append(s, fmt.Sprintf(`create table %v as select * from all_objects where 1=0%v`, tableName, terminator))
	s = append(s, fmt.Sprintf(`alter table %v add (last_modified_date date)%v`, tableName, terminator))
	s = append(s, fmt.Sprintf(`insert into %v (
  select * from (  -- grab some data from ALL_OBJECTS...
    (select * from all_objects where rownum <= 10000)
    cross join  -- generate a range of dates...
    (select sysdate - 10 + level last_modified_date
      from dual
      connect by level <= 10)
    )
)%v`, tableName, terminator))
	s = append(s, fmt.Sprintf(`commit%v`, terminator))
	return s
}

func getOracleDemoTableCleanupDDL(tableName string, addTerminator bool) []string {
	terminator := ""
	if addTerminator {
		terminator = " /"
	}
	return []string{
		fmt.Sprintf("begin execute immediate 'drop table %v purge'; exception when others then null; end;%v", tableName, terminator),
	}[:]
}

func getSnowflakeDemoTableDDL(tableName string, addTerminator bool) []string {
	terminator := ""
	if addTerminator {
		terminator = ";"
	}
	s := [2]string{} // make([]string, 2, 2)
	s[0] = fmt.Sprintf(`drop table if exists %v%v`, tableName, terminator)
	s[1] = fmt.Sprintf(`create table %v ( 
  OWNER varchar(128), 
  OBJECT_NAME varchar(128), 
  SUBOBJECT_NAME varchar(128), 
  OBJECT_ID number, 
  DATA_OBJECT_ID number, 
  OBJECT_TYPE varchar(23), 
  CREATED datetime, 
  LAST_DDL_TIME datetime, 
  TIMESTAMP varchar(19), 
  STATUS varchar(7), 
  TEMPORARY varchar(1), 
  GENERATED varchar(1), 
  SECONDARY varchar(1), 
  NAMESPACE number, 
  EDITION_NAME varchar(128), 
  SHARING varchar(18), 
  EDITIONABLE varchar(1), 
  ORACLE_MAINTAINED varchar(1), 
  APPLICATION varchar(1), 
  DEFAULT_COLLATION varchar(100), 
  DUPLICATED varchar(1), 
  SHARDED varchar(1), 
  CREATED_APPID number, 
  CREATED_VSNID number, 
  MODIFIED_APPID number, 
  MODIFIED_VSNID number, 
  LAST_MODIFIED_DATE datetime)%v`, tableName, terminator)

	return s[:]
}

func getSnowflakeDemoTableCleanupDDL(tableName string, addTerminator bool) []string {
	terminator := ""
	if addTerminator {
		terminator = ";"
	}
	return []string{fmt.Sprintf("drop table if exists %v%v", tableName, terminator)}[:]
}

func getSnowflakeStageDDL(stageName string, s3Url string, key string, secret string, addTerminator bool) []string {
	terminator := ""
	s3Url = "s3://" + strings.TrimPrefix(s3Url, "s3://") // ensure 's3://' leading string.
	if addTerminator {
		terminator = ";"
	}
	s := [1]string{}
	s[0] = fmt.Sprintf(`create or replace stage %v
  url = '%v'
  credentials = ( 
    aws_key_id = '%v' 
    aws_secret_key = '%v' 
  )
  file_format = (
    type = csv
    compression = gzip
    skip_header = 1
    date_format = 'YYYYMMDD"T"HH24MISSTZHTZM'
    timestamp_format = 'YYYYMMDD"T"HH24MISSFF9TZHTZM'
    field_optionally_enclosed_by = '"'
  )
  comment = 'HalfPipe command-line tool'%v`,
		stageName,
		s3Url, key, secret,
		terminator)
	return s[:]
}

func getSnowflakeStageCleanupDDL(stageName string, addTerminator bool) []string {
	terminator := ""
	if addTerminator {
		terminator = ";"
	}
	return []string{fmt.Sprintf("drop stage if exists %v%v", stageName, terminator)}[:]
}
