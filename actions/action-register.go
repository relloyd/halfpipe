package actions

import (
	"fmt"
	"reflect"

	"github.com/relloyd/halfpipe/constants"
)

type SrcAndTgtConnections struct {
	Connections  ConnectionHandler
	SourceString ConnectionObject
	TargetString ConnectionObject
}

type Action struct {
	FnAction   func(actionCfg interface{}) error                         // the function to execute the action
	ActionCfg  interface{}                                               // the config struct to pass to the FnAction
	FnSetupCfg func(genericCfg interface{}, actionCfg interface{}) error // the function to convert generic cfg to action-specific config for the FnAction
}

// ActionLauncher will:
// 1) call the function fnActionGetter to find the Action{} based on the sourceType and targetType strings supplied.
// 2) Once it has the Action{}, it calls setup function Action.FnSetupCfg() to populate Action.ActionCfg{}.
// 3) Then it can start the action by calling Action.FnAction().
// TODO: consider moving use of fnActionGetter out to the caller such that the caller supplies a fn(void) to call all
//  preconfigured ready to go.
func ActionLauncher(
	cfg interface{},
	fnActionGetter func(sourceType string, targetType string) (Action, error),
	sourceType string,
	targetType string) error {
	v := reflect.ValueOf(cfg)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("expected pointer to config in variable cfg to be supplied to ActionLauncher")
	}
	// Fetch the action.
	a, err := fnActionGetter(sourceType, targetType)
	if err != nil {
		return err
	}
	// Populate the action's config struct using the generic.
	if err = a.FnSetupCfg(cfg, a.ActionCfg); err != nil {
		return err
	}
	// Run the action.
	return a.FnAction(a.ActionCfg)
}

// ActionFuncs is a register of all supported actions.
// Note that keys in the final map[string]Action are used to validate DSN-type database connections before
// they are added. See RunConnectionAdd().
var ActionFuncs = map[string]map[string]map[string]Action{
	constants.ActionFuncsCommandCp: { // command...
		constants.ActionFuncsSubCommandSnapshot: { // subcommand...
			// Snapshots can run without DATE/NUMBER arithmetic so any ODBC is good.
			"oracle-oracle":            Action{FnAction: RunOracleOracleSnapshot, ActionCfg: &OraOraSnapConfig{}, FnSetupCfg: SetupCpOraOraSnap},
			"oracle-s3":                Action{FnAction: RunOracleS3Snapshot, ActionCfg: &OraS3SnapConfig{}, FnSetupCfg: SetupCpOraS3Snap},
			"oracle-snowflake":         Action{FnAction: RunSnowflakeSnapshot, ActionCfg: &OraSnowflakeSnapConfig{}, FnSetupCfg: SetupCpOraSnowflakeSnap},
			"oracle-sqlserver":         Action{FnAction: RunDsnSnapshot, ActionCfg: &DsnSnapConfig{}, FnSetupCfg: SetupCpDsnSnap},
			"sqlserver-sqlserver":      Action{FnAction: RunDsnSnapshot, ActionCfg: &DsnSnapConfig{}, FnSetupCfg: SetupCpDsnSnap},
			"sqlserver-s3":             Action{FnAction: RunDsnS3Snapshot, ActionCfg: &DsnS3SnapConfig{}, FnSetupCfg: SetupCpDsnS3Snap},
			"sqlserver-snowflake":      Action{FnAction: RunDsnSnowflakeSnapshot, ActionCfg: &DsnSnowflakeSnapConfig{}, FnSetupCfg: SetupCpDsnSnowflakeSnap},
			"sqlserver-oracle":         Action{FnAction: RunDsnSnapshot, ActionCfg: &DsnSnapConfig{}, FnSetupCfg: SetupCpDsnSnap},
			"odbc+sqlserver-s3":        Action{FnAction: RunDsnS3Snapshot, ActionCfg: &DsnS3SnapConfig{}, FnSetupCfg: SetupCpDsnS3Snap},
			"odbc+sqlserver-snowflake": Action{FnAction: RunDsnSnowflakeSnapshot, ActionCfg: &DsnSnowflakeSnapConfig{}, FnSetupCfg: SetupCpDsnSnowflakeSnap},
			"s3-snowflake":             Action{FnAction: RunS3SnowflakeSnapshot, ActionCfg: &S3SnowflakeSnapConfig{}, FnSetupCfg: SetupCpS3SnowflakeSnap},
			"s3-stdout":                Action{FnAction: RunS3StdoutSnapshot, ActionCfg: &S3StdoutSnapConfig{}, FnSetupCfg: SetupCpS3StdoutSnap},
			"netezza-s3":               Action{FnAction: RunDsnS3Snapshot, ActionCfg: &DsnS3SnapConfig{}, FnSetupCfg: SetupCpDsnS3Snap},
			"netezza-snowflake":        Action{FnAction: RunDsnSnowflakeSnapshot, ActionCfg: &DsnSnowflakeSnapConfig{}, FnSetupCfg: SetupCpDsnSnowflakeSnap},
		},
		constants.ActionFuncsSubCommandDelta: {
			// TODO: add commented delta actions!
			// Deltas require DATE/NUMBER arithmetic so this needs modules rdbms to provide appropriate SQL snippet.
			"oracle-oracle":    Action{FnAction: RunOracleOracleDelta, ActionCfg: &OraOraDeltaConfig{}, FnSetupCfg: SetupCpOraOraDelta},
			"oracle-snowflake": Action{FnAction: RunSnowflakeDelta, ActionCfg: &OraSnowflakeDeltaConfig{}, FnSetupCfg: SetupCpOraSnowflakeDelta},
			"oracle-s3":        Action{FnAction: RunOracleS3Delta, ActionCfg: &OraS3DeltaConfig{}, FnSetupCfg: SetupCpOraS3Delta},
			// "oracle-sqlserver":         Action{FnAction: , ActionCfg: &OraS3DeltaConfig{}, FnSetupCfg: SetupCpOraS3Delta},
			// "sqlserver-sqlserver":             Action{FnAction: , ActionCfg: &DsnS3DeltaConfig{}, FnSetupCfg: SetupCpDsnS3Delta},
			"sqlserver-s3":        Action{FnAction: RunDsnS3Delta, ActionCfg: &DsnS3DeltaConfig{}, FnSetupCfg: SetupCpDsnS3Delta},
			"sqlserver-snowflake": Action{FnAction: RunDsnSnowflakeDelta, ActionCfg: &DsnSnowflakeDeltaConfig{}, FnSetupCfg: SetupCpDsnSnowflakeDelta},
			// "sqlserver-oracle":      Action{FnAction: , ActionCfg: &DsnSnowflakeDeltaConfig{}, FnSetupCfg: SetupCpDsnSnowflakeDelta},
			"odbc+sqlserver-s3":        Action{FnAction: RunDsnS3Delta, ActionCfg: &DsnS3DeltaConfig{}, FnSetupCfg: SetupCpDsnS3Delta},
			"odbc+sqlserver-snowflake": Action{FnAction: RunDsnSnowflakeDelta, ActionCfg: &DsnSnowflakeDeltaConfig{}, FnSetupCfg: SetupCpDsnSnowflakeDelta},
			"s3-snowflake":             Action{FnAction: RunS3SnowflakeDelta, ActionCfg: &S3SnowflakeDeltaConfig{}, FnSetupCfg: SetupCpS3SnowflakeDelta},
			// "s3-stdout":             Action{FnAction: , ActionCfg: &S3SnowflakeDeltaConfig{}, FnSetupCfg: SetupCpS3SnowflakeDelta},
		},
		constants.ActionFuncsSubCommandMeta: { // meta...
			// Requires support in module table-definition.
			// Connections of type ODBC are only supported if the correct 'transport' is used in specific entries below.
			"oracle-snowflake":         Action{FnAction: RunTableToSnowflakeDDL, ActionCfg: &TableToSnowTableDDLConfig{}, FnSetupCfg: SetupCpMetaToSnowflake},
			"sqlserver-snowflake":      Action{FnAction: RunTableToSnowflakeDDL, ActionCfg: &TableToSnowTableDDLConfig{}, FnSetupCfg: SetupCpMetaToSnowflake},
			"netezza-snowflake":        Action{FnAction: RunTableToSnowflakeDDL, ActionCfg: &TableToSnowTableDDLConfig{}, FnSetupCfg: SetupCpMetaToSnowflake},
			"odbc+sqlserver-snowflake": Action{FnAction: RunTableToSnowflakeDDL, ActionCfg: &TableToSnowTableDDLConfig{}, FnSetupCfg: SetupCpMetaToSnowflake},
		},
	},
	constants.ActionFuncsCommandSync: {
		constants.ActionFuncsSubCommandBatch: {
			// Can run without DATE arithmetic so any ODBC is good.
			"oracle-oracle":    Action{FnAction: RunOracleOracleSync, ActionCfg: &SyncOracleOracleConfig{}, FnSetupCfg: SetupOracleToOracleSync},
			"oracle-snowflake": Action{FnAction: RunOracleSnowflakeSync, ActionCfg: &SyncOracleSnowflakeConfig{}, FnSetupCfg: SetupOracleToSnowflakeSync},
			// "oracle-sqlserver": Action{FnAction: , ActionCfg: &SyncOracleSnowflakeConfig{}, FnSetupCfg: SetupOracleToSnowflakeSync},
			// "sqlserver-oracle": Action{FnAction: , ActionCfg: &SyncOracleSnowflakeConfig{}, FnSetupCfg: SetupOracleToSnowflakeSync},
		},
		constants.ActionFuncsSubCommandEvents: {
			// The source system needs to support continuous query notifications.
			// TODO: add SQL Server handling of CQN.
			"oracle-oracle":    Action{FnAction: RunCqnOracleSync, ActionCfg: &SyncCqnOracleConfig{}, FnSetupCfg: SetupCqnToOracleSync},
			"oracle-snowflake": Action{FnAction: RunCqnSnowflakeSync, ActionCfg: &SyncCqnSnowflakeConfig{}, FnSetupCfg: SetupCqnToSnowflakeSync},
		},
	},
}

// GetCpSnapAction returns the "cp snap" Action based on sourceType and targetTypes supplied.
func GetCpSnapAction(sourceType string, targetType string) (Action, error) {
	retval, ok := ActionFuncs[constants.ActionFuncsCommandCp][constants.ActionFuncsSubCommandSnapshot][sourceType+"-"+targetType]
	if !ok {
		return Action{}, fmt.Errorf("unsupported cp snap action for source type %q and target type %q", sourceType, targetType)
	}
	return retval, nil
}

// GetCpDeltaAction returns the "cp delta" Action based on sourceType and targetTypes supplied.
func GetCpDeltaAction(sourceType string, targetType string) (Action, error) {
	retval, ok := ActionFuncs[constants.ActionFuncsCommandCp][constants.ActionFuncsSubCommandDelta][sourceType+"-"+targetType]
	if !ok {
		return Action{}, fmt.Errorf("unsupported cp delta action for source type %q and target type %q", sourceType, targetType)
	}
	return retval, nil
}

// GetCpMetaAction returns the "cp meta" Action based on sourceType and targetTypes supplied.
func GetCpMetaAction(sourceType string, targetType string) (Action, error) {
	retval, ok := ActionFuncs[constants.ActionFuncsCommandCp][constants.ActionFuncsSubCommandMeta][sourceType+"-"+targetType]
	if !ok {
		return Action{}, fmt.Errorf("unsupported cp meta action for source type %q and target type %q", sourceType, targetType)
	}
	return retval, nil
}

// GetSyncBatchAction returns the "sync" Action based on sourceType and targetTypes supplied.
func GetSyncBatchAction(sourceType string, targetType string) (Action, error) {
	retval, ok := ActionFuncs[constants.ActionFuncsCommandSync][constants.ActionFuncsSubCommandBatch][sourceType+"-"+targetType]
	if !ok {
		return Action{}, fmt.Errorf("unsupported sync action for source type %q and target type %q", sourceType, targetType)
	}
	return retval, nil
}

// GetSyncBatchAction returns the "sync" Action based on sourceType and targetTypes supplied.
func GetSyncEventsAction(sourceType string, targetType string) (Action, error) {
	retval, ok := ActionFuncs[constants.ActionFuncsCommandSync][constants.ActionFuncsSubCommandEvents][sourceType+"-"+targetType]
	if !ok {
		return Action{}, fmt.Errorf("unsupported sync action for source type %q and target type %q", sourceType, targetType)
	}
	return retval, nil
}
