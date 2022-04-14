package actions

import (
	"fmt"
	"strings"

	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	td "github.com/relloyd/halfpipe/table-definition"
)

type TableToSnowTableDDLConfig struct {
	SrcSchemaTable   rdbms.SchemaTable
	SnowSchemaTable  rdbms.SchemaTable
	SrcConnDetails   *shared.ConnectionDetails
	TgtConnDetails   *shared.ConnectionDetails
	ExecuteDDL       bool
	LogLevel         string
	StackDumpOnPanic bool
}

type StageDDLConfig struct {
	LogLevel string
}

type TriggerDDLConfig struct {
	LogLevel       string
	SchemaTable    rdbms.SchemaTable `errorTxt:"[schema.]table" mandatory:"yes"`
	SchemaTrigger  OraSchemaTrigger  `errorTxt:"[schema.]trigger" mandatory:"yes"`
	DateColumnName string            `errorTxt:"DATE column name" mandatory:"yes"`
}

type OraSchemaTrigger struct {
	SchemaTrigger string `errorTxt:"[schema.]trigger" mandatory:"yes"`
}

// SetupCpMetaToSnowflake copies required properties from genericCfg to actionCfg of type TableToSnowTableDDLConfig
// ready to execute RunTableToSnowflakeDDL().
func SetupCpMetaToSnowflake(genericCfg interface{}, actionCfg interface{}) error {
	src := genericCfg.(*CpConfig)
	tgt := actionCfg.(*TableToSnowTableDDLConfig)
	var err error
	// Setup real connection details.
	if tgt.SrcConnDetails, err = src.Connections.GetConnectionDetails(src.SourceString.GetConnectionName()); err != nil {
		return err
	}
	if tgt.TgtConnDetails, err = src.Connections.GetConnectionDetails(src.TargetString.GetConnectionName()); err != nil {
		return err
	}
	tgt.LogLevel = src.LogLevel
	tgt.StackDumpOnPanic = src.StackDumpOnPanic
	tgt.SrcSchemaTable.SchemaTable = src.SourceString.GetObject()
	tgt.SnowSchemaTable.SchemaTable = src.TargetString.GetObject()
	tgt.ExecuteDDL = src.ExecuteDDL
	return nil
}

// OracleTableToSnowflakeDDLFunc returns a function that will do ths work of:
// 1) Read Oracle schema.table definition and generate equivalent Snowflake table creation statement.
// It prints the DDL to STDOUT.
func RunTableToSnowflakeDDL(cfg interface{}) error {
	ddlCfg := cfg.(*TableToSnowTableDDLConfig)
	// Setup logging.
	if ddlCfg.LogLevel == "" {
		ddlCfg.LogLevel = "info"
	}
	if ddlCfg.SnowSchemaTable.GetTable() == "" { // if the user has not supplied a target table name...
		ddlCfg.SnowSchemaTable.SchemaTable = ddlCfg.SrcSchemaTable.GetTable() // default to the source table name
	}
	log := logger.NewLogger("halfpipe", ddlCfg.LogLevel, ddlCfg.StackDumpOnPanic)
	log.Debug("schema.table='", ddlCfg.SrcSchemaTable, "'")
	// Validate the input switches / cfg.
	err := validateCpMetaConfig(ddlCfg)
	if err != nil {
		return err
	}
	// Get the function that fetches column definitions and a mapper for the database connection type.
	fnGetColumns := td.GetColumnsFunc(ddlCfg.SrcConnDetails)
	mapper := td.MustGetMapper(ddlCfg.SrcConnDetails)
	// Run the action.
	tabDefinition, err := td.GetTableDefinition(log, fnGetColumns, &ddlCfg.SrcSchemaTable)
	if err != nil {
		return err
	}
	// Convert the table definition to Snowflake.
	ddl, err := td.ConvertTableDefinitionToSnowflake(log, tabDefinition, ddlCfg.SnowSchemaTable, mapper)
	// Print and/or execute the DDL.
	printLogFn := getPrintLogFunc(log, !ddlCfg.ExecuteDDL)
	printLogFn(strings.TrimRight(ddl, ";"+";"))
	if ddlCfg.ExecuteDDL { // if we should execute the DDL...
		fn := func() error {
			return rdbms.SnowflakeDDLExec(log, shared.GetDsnConnectionDetails(ddlCfg.TgtConnDetails), ddl)
		}
		mustExecFn(log, printLogFn, fn)
	}
	return nil
}

// validateCpMetaConfig builds an error string showing which fields of cfg must be populated based on struct tags.
// This ignores validation of source database config struct since this may vary by database type.
// We are assuming that connection details are validated on their way into the config store!
func validateCpMetaConfig(cfg *TableToSnowTableDDLConfig) (err error) {
	errs := make([]string, 0)
	snowConn := shared.GetDsnConnectionDetails(cfg.TgtConnDetails)
	helper.GetStructErrorTxt4UnsetFields(cfg.SrcSchemaTable, &errs)
	if cfg.ExecuteDDL {
		helper.GetStructErrorTxt4UnsetFields(snowConn, &errs)
		helper.GetStructErrorTxt4UnsetFields(cfg.SnowSchemaTable, &errs)
	}
	if len(errs) > 0 {
		err = fmt.Errorf("please supply values for %v", strings.Join(errs, ", "))
	}
	return
}

// Richard 20200925 - comment unused func:
// SnowflakeCreateStageDDLFunc returns a func that will print to STDOUT a Snowflake CREATE STAGE DDL statement.
// func SnowflakeCreateStageDDLFunc(cfg *StageDDLConfig) func(c *cli.Context) error {
// 	return func(c *cli.Context) error {
// 		// Setup logging.
// 		if cfg.LogLevel == "" {
// 			cfg.LogLevel = "error"
// 		}
// 		log := logger.NewLogger("halfpipe", cfg.LogLevel, true)
// 		err := helper.ValidateStructIsPopulated(cfg)
// 		if err != nil {
// 			log.Panic(err)
// 		}
// 		ddl := getSnowflakeStageDDL(
// 			"<stage name>",
// 			"s3://<S3-bucket-name>",
// 			"<AWS key>",
// 			"<AWS secret>",
// 			true)
// 		log.Info(ddl)
// 		fmt.Println(ddl)
// 		return nil
// 	}
// }

// Richard 20200925 - comment unused func:
// OracleCreateTriggerDDLFunc returns a func that will print to STDOUT a Snowflake CREATE STAGE DDL statement.
// func OracleCreateTriggerDDLFunc(cfg *TriggerDDLConfig) func(c *cli.Context) error {
// 	return func(c *cli.Context) error {
// 		// Setup logging.
// 		if cfg.LogLevel == "" {
// 			cfg.LogLevel = "error"
// 		}
// 		log := logger.NewLogger("halfpipe", cfg.LogLevel, true)
// 		err := helper.ValidateStructIsPopulated(cfg)
// 		if err != nil {
// 			log.Panic(err)
// 		}
// 		ddl := getOracleTriggerDDL(cfg.SchemaTable.SchemaTable, cfg.SchemaTrigger.SchemaTrigger, cfg.DateColumnName, true)
// 		log.Info(ddl)
// 		for _, stmt := range ddl {
// 			fmt.Println(stmt)
// 		}
// 		return nil
// 	}
// }

// Richard 20190315 - comment demo DDL func as it is available in the demo action anyway.
// func OracleCreateDemoTableDDLFunc(cfg *DemoTableDDLConfig) (func(c *cli.Context) error) {
// 	return func(c *cli.Context) error {
// 		// Setup logging.
// 		if cfg.LogLevel == "" {
// 			cfg.LogLevel = "error"
// 		}
// 		log := logger.NewLogger("halfpipe", cfg.LogLevel)
// 		err := validateDemoTableDDLConfig(log, cfg)
// 		if err != nil {
// 			log.Panic(err)
// 		}
// 		ddl := getOracleDemoTableDDL(cfg.SchemaTable.SchemaTable, false)
// 		log.Debug(ddl)
// 		for _, stmt := range ddl {
// 			fmt.Println(stmt)
// 		}
// 		return err
// 	}
// }
//
// func validateDemoTableDDLConfig(log logger.Logger, cfg *DemoTableDDLConfig) (err error) {
// 	errs := make([]string, 0)
// 	getStructErrorTxt4UnsetFields(log, cfg, &errs)
// 	if len(errs) > 0 {
// 		err = fmt.Errorf("Please supply values for %v", strings.Join(errs, ", "))
// 	}
// 	return
// }
