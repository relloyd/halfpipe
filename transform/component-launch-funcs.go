package transform

import (
	"os"
	"strconv"

	"github.com/relloyd/halfpipe/components"
	"github.com/relloyd/halfpipe/helper"
	"github.com/relloyd/halfpipe/logger"
	"github.com/relloyd/halfpipe/rdbms"
	"github.com/relloyd/halfpipe/rdbms/shared"
	"github.com/relloyd/halfpipe/stream"
)

// TODO: simplify/consolidate the fact that getComponentWaiter() and setStepControlChan() must receive the same keys per step else shutdown() doesn't work!

func startMetaDataInjection(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	cfg := &MetadataInjectionConfig{
		Log:                                 log,
		Name:                                stepCanonicalName,
		InputChan:                           sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		GlobalTransformManager:              sgm.getGlobalTransformManager(),
		TransformGroupName:                  sg.Steps[stepName].Data["executeTransformName"],
		ReplacementVariableWithFieldNameCSV: sg.Steps[stepName].Data["replaceVariableWithFieldNameCSV"],
		ReplacementDateTimeFormat:           sg.Steps[stepName].Data["replaceDateTimeUsingFormat"],
		OutputChanFieldName4JSON:            "mdiOutput",
		StepWatcher:                         stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                         sgm.getComponentWaiter(stepName),
		PanicHandlerFn:                      panicHandlerFn}
	// Create new MetadataInjection step and save its output channel.
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startSqlExec(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	cfg := &components.SqlExecConfig{
		Log:                      log,
		Name:                     stepCanonicalName,
		InputChan:                sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		SqlQueryFieldName:        sg.Steps[stepName].Data["sqlQueryFieldName"],
		SqlRowsAffectedFieldName: sg.Steps[stepName].Data["sqlRowsAffectedFieldName"],
		OutputDb:                 sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["databaseConnectionName"]),
		StepWatcher:              stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:              sgm.getComponentWaiter(stepName),
		PanicHandlerFn:           panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)      // TODO: supply args from previous step as input here.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"]) // TODO: remove need to call consumeStep(); move this to getStepOutputChan()
}

func startTableInput(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Create and save the TableInput channel containing SQL results.
	cfg := &components.SqlQueryWithArgsConfig{
		Log:            log,
		Name:           stepCanonicalName,
		Db:             sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["databaseConnectionName"]),
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		Sqltext:        sg.Steps[stepName].Data["sqlText"],
		Args:           nil,
		PanicHandlerFn: panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)      // TODO: supply args from previous step as input here.
}

func startTableInputWithArgs(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(i interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Create and save the TableInput channel containing SQL results.
	readDataFromStepName := sg.Steps[stepName].Data["readDataFromStep"]
	log.Debug("Creating TableInputWithArgs (", stepName, ") reading data from step ", readDataFromStepName, "...")
	cfg := &components.SqlQueryWithChanConfig{
		Log:  log,
		Name: stepCanonicalName,
		Db:   sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["databaseConnectionName"]),
		// InputChan:       sgm.mapOutputChans[readDataFromStepName],
		InputChan:       sgm.getStepOutputChan(readDataFromStepName),
		InputChanFields: helper.CsvToStringSliceTrimSpaces(sg.Steps[stepName].Data["readDataUsingFields"]),
		Sqltext:         sg.Steps[stepName].Data["sqlText"],
		StepWatcher:     stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:     sgm.getComponentWaiter(stepName),
		PanicHandlerFn:  panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)      // TODO: supply args from previous step as input here.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startTableInputWithReplacements(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Create and save the TableInput channel containing SQL results.
	mapTokens, err := helper.CsvStringOfTokensToMap(log, sg.Steps[stepName].Data["replacements"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to parse SQL string replacements: ", err)
	}
	cfg := &components.SqlQueryWithReplace{
		Log:            log,
		Name:           stepCanonicalName,
		Db:             sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["databaseConnectionName"]),
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		Sqltext:        sg.Steps[stepName].Data["sqlText"],
		Replacements:   mapTokens,
		PanicHandlerFn: panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)      // TODO: supply args from previous step as input here.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startTableInputCqnToRdbms(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Create CQN connection.
	// TODO: move CQN connection to a sensible location where it can be closed properly.
	srcDBConnectionDetails := sgm.getGlobalTransformManager().getDBConnectionDetails(sg.Steps[stepName].Data["sourceDatabaseConnectionName"]) // get raw connection details from input transform / JSON
	srcOracleDsn := shared.GetDsnConnectionDetails(&srcDBConnectionDetails)                                                                   // build Oracle connection details; see also shared.GetOracleConnectionDetailsWithEnv
	// Richard Lloyd 2020-11-11: Oracle connections now use DSN instead of connection specific struct.
	// srcOracleConnectionDetails := shared.GetOracleConnectionDetails(&srcDBConnectionDetails)                                                  // build Oracle connection details; see also shared.GetOracleConnectionDetailsWithEnv
	// if err != nil {
	// 	log.Panic("error building connection details for CQN: ", err)
	// }
	// srcDsn, err := shared.OracleConnectionDetailsToDSN(srcOracleConnectionDetails)
	// if err != nil {
	// 	log.Panic("error building connect string for CQN source database: ", err)
	// }
	srcCqnConn, err := rdbms.NewCqnConnection(srcOracleDsn.Dsn)
	if err != nil {
		log.Panic(stepCanonicalName, " error: ", err)
	}
	// Create CQN downstream handler.
	batchSize, err := strconv.Atoi(sg.Steps[stepName].Data["batchSize"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to parse CQN batchSize: ", err)
	}
	downstreamHandler := &components.CqnDownstreamTableSync{BatchSize: batchSize} // TODO: separate batch size vs commit size required in CQN component!
	// Create and save the TableInput channel containing SQL results.
	cfg := &components.CqnWithArgsConfig{
		Log:                  log,
		Name:                 stepCanonicalName,
		SrcCqnConnection:     srcCqnConn,
		SrcDBConnector:       sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["sourceDatabaseConnectionName"]),
		TgtDBConnector:       sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["targetDatabaseConnectionName"]),
		SrcSqlQuery:          sg.Steps[stepName].Data["sourceSqlQuery"],
		TgtSqlQuery:          sg.Steps[stepName].Data["targetSqlQuery"],
		SrcRowIdKey:          sg.Steps[stepName].Data["sourceRowIdColumnName"],
		TgtRowIdKey:          sg.Steps[stepName].Data["targetRowIdColumnName"],
		TgtSchema:            sg.Steps[stepName].Data["targetSchemaName"],
		TgtTable:             sg.Steps[stepName].Data["targetTableName"],
		TgtKeyCols:           helper.TokensToOrderedMap(sg.Steps[stepName].Data["primaryKeyColumns"]),
		TgtOtherCols:         helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherColumns"]),
		MergeDiffJoinKeys:    helper.TokensToOrderedMap(sg.Steps[stepName].Data["primaryKeyColumns"]),
		MergeDiffCompareKeys: helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherColumns"]),
		StepWatcher:          stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:          sgm.getComponentWaiter(stepName),
		PanicHandlerFn:       panicHandlerFn,
		DownstreamHandler:    downstreamHandler,
	}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
}

func startTableInputCqnToSnowflake(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Create CQN connection.
	// TODO: move CQN connection to a sensible location where it can be closed properly.
	srcDBConnectionDetails := sgm.getGlobalTransformManager().getDBConnectionDetails(sg.Steps[stepName].Data["sourceDatabaseConnectionName"]) // get raw connection details from input transform / JSON
	srcOracleDsn := shared.GetDsnConnectionDetails(&srcDBConnectionDetails)                                                                   // build Oracle connection details; see also shared.GetOracleConnectionDetailsWithEnv
	// Richard Lloyd 2020-11-11: Oracle connections now use DSN instead of connection specific struct.
	// srcOracleConnectionDetails := shared.GetOracleConnectionDetails(&srcDBConnectionDetails)                                                  // build Oracle connection details; see also shared.GetOracleConnectionDetailsWithEnv
	// if err != nil {
	// 	log.Panic("error building connection details for CQN: ", err)
	// }
	// srcDsn, err := shared.OracleConnectionDetailsToDSN(srcOracleConnectionDetails)
	// if err != nil {
	// 	log.Panic("error building connect string for CQN source database: ", err)
	// }
	srcCqnConn, err := rdbms.NewCqnConnection(srcOracleDsn.Dsn)
	if err != nil {
		log.Panic(err)
	}
	// Create CQN downstream handler for Snowflake.
	csvMaxFileRows, err := strconv.Atoi(sg.Steps[stepName].Data["csvMaxFileRows"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to parse the CQN csvMaxFileRows - check it exists in the pipe config: ", err)
	}
	csvMaxFileBytes, err := strconv.Atoi(sg.Steps[stepName].Data["csvMaxFileBytes"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to parse the CQN csvMaxFileBytes - check it exists in the pipe config: ", err)
	}
	downstreamHandler := &components.CqnDownstreamSnowflakeSync{
		StageName:         sg.Steps[stepName].Data["snowflakeStageName"],
		Region:            sg.Steps[stepName].Data["bucketRegion"],
		BucketPrefix:      sg.Steps[stepName].Data["bucketPrefix"],
		BucketName:        sg.Steps[stepName].Data["bucketName"],
		CsvFileNamePrefix: sg.Steps[stepName].Data["csvFileNamePrefix"],
		CsvHeaderFields:   helper.CsvToStringSliceTrimSpacesRemoveQuotes(sg.Steps[stepName].Data["csvHeaderFieldsCSV"]), // TODO: make this use a safe csv reader.
		CsvMaxFileRows:    csvMaxFileRows,
		CsvMaxFileBytes:   csvMaxFileBytes,
	}
	// Create and save the TableInput channel containing SQL results.
	cfg := &components.CqnWithArgsConfig{
		Log:                  log,
		Name:                 stepCanonicalName,
		SrcCqnConnection:     srcCqnConn,
		SrcDBConnector:       sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["sourceDatabaseConnectionName"]),
		TgtDBConnector:       sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["targetDatabaseConnectionName"]),
		SrcSqlQuery:          sg.Steps[stepName].Data["sourceSqlQuery"],
		TgtSqlQuery:          sg.Steps[stepName].Data["targetSqlQuery"],
		SrcRowIdKey:          sg.Steps[stepName].Data["sourceRowIdColumnName"],
		TgtRowIdKey:          sg.Steps[stepName].Data["targetRowIdColumnName"],
		TgtSchema:            sg.Steps[stepName].Data["targetSchemaName"],
		TgtTable:             sg.Steps[stepName].Data["targetTableName"],
		TgtKeyCols:           helper.TokensToOrderedMap(sg.Steps[stepName].Data["primaryKeyColumns"]),
		TgtOtherCols:         helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherColumns"]),
		MergeDiffJoinKeys:    helper.TokensToOrderedMap(sg.Steps[stepName].Data["primaryKeyColumns"]),
		MergeDiffCompareKeys: helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherColumns"]),
		StepWatcher:          stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:          sgm.getComponentWaiter(stepName),
		PanicHandlerFn:       panicHandlerFn,
		DownstreamHandler:    downstreamHandler,
	}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
}

func startMergeDiff(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Create and save the MergeDiff output channel.
	outputIdenticalRows, _ := strconv.ParseBool(sg.Steps[stepName].Data["outputIdenticalRows"])
	cfg := &components.MergeDiffConfig{
		Log:                 log,
		Name:                stepCanonicalName,
		ChanOld:             sgm.getStepOutputChan(sg.Steps[stepName].Data["readOldDataFromStep"]),
		ChanNew:             sgm.getStepOutputChan(sg.Steps[stepName].Data["readNewDataFromStep"]),
		JoinKeys:            helper.TokensToOrderedMap(sg.Steps[stepName].Data["joinKeys"]),
		CompareKeys:         helper.TokensToOrderedMap(sg.Steps[stepName].Data["compareKeys"]),
		ResultFlagKeyName:   sg.Steps[stepName].Data["flagFieldName"],
		OutputIdenticalRows: outputIdenticalRows,
		StepWatcher:         stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:         sgm.getComponentWaiter(stepName),
		PanicHandlerFn:      panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readOldDataFromStep"])
	sgm.consumeStep(sg.Steps[stepName].Data["readNewDataFromStep"])
}

func startTableSync(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Start a TableSync step using inputs.
	commitBatchSize, err := strconv.Atoi(sg.Steps[stepName].Data["commitBatchSize"])
	if err != nil {
		log.Panic(stepCanonicalName, " error extracting commitBatchSize from TableSync: ", err)
	}
	txtBatchNumRows, err := strconv.Atoi(sg.Steps[stepName].Data["txtBatchNumRows"])
	if err != nil {
		log.Panic(stepCanonicalName, " error extracting txtBatchNumRows from TableSync: ", err)
	}
	cfg := &components.TableSyncConfig{
		Log:             log,
		Name:            stepCanonicalName,
		CommitBatchSize: commitBatchSize,
		TxtBatchNumRows: txtBatchNumRows,
		FlagKeyName:     sg.Steps[stepName].Data["flagFieldName"],
		InputChan:       sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		OutputDb:        sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["databaseConnectionName"]),
		SqlStatementGeneratorConfig: shared.SqlStatementGeneratorConfig{
			Log:             log,
			OutputSchema:    sg.Steps[stepName].Data["outputSchemaName"],
			SchemaSeparator: ".", // TODO: do we externalise this separator?
			OutputTable:     sg.Steps[stepName].Data["outputTable"],
			TargetKeyCols:   helper.TokensToOrderedMap(sg.Steps[stepName].Data["keyCols"]),
			TargetOtherCols: helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherCols"])},
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	// Create and save the TableSync output channel.
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startTableMerge(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Start a TableMerge step using inputs.
	execBatchSize, err1 := strconv.Atoi(sg.Steps[stepName].Data["execBatchSize"])
	if err1 != nil {
		log.Panic(stepCanonicalName, " error extracting execBatchSize from TableMerge: ", err1)
	}
	commitBatchSize, err2 := strconv.Atoi(sg.Steps[stepName].Data["commitBatchSize"])
	if err2 != nil {
		log.Panic(stepCanonicalName, " error extracting commitBatchSize from TableMerge: ", err2)

	}
	cfg := &components.TableMergeConfig{
		Log:             log,
		Name:            stepCanonicalName,
		InputChan:       sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		OutputDb:        sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["databaseConnectionName"]),
		CommitBatchSize: commitBatchSize,
		ExecBatchSize:   execBatchSize,
		SqlStatementGeneratorConfig: shared.SqlStatementGeneratorConfig{
			Log:             log,
			SchemaSeparator: ".", // TODO: do we externalise this separator?
			OutputSchema:    sg.Steps[stepName].Data["outputSchemaName"],
			OutputTable:     sg.Steps[stepName].Data["outputTable"],
			TargetKeyCols:   helper.TokensToOrderedMap(sg.Steps[stepName].Data["keyCols"]),
			TargetOtherCols: helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherCols"])},
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	// Create and save the TableSync output channel.
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startS3BucketList(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Set defaults if missing inputs.
	if sg.Steps[stepName].Data["outputField4BucketName"] == "" {
		sg.Steps[stepName].Data["outputField4BucketName"] = components.Defaults.ChanField4BucketName
	}
	if sg.Steps[stepName].Data["outputField4BucketPrefix"] == "" {
		sg.Steps[stepName].Data["outputField4BucketPrefix"] = components.Defaults.ChanField4BucketPrefix
	}
	if sg.Steps[stepName].Data["outputField4BucketRegion"] == "" {
		sg.Steps[stepName].Data["outputField4BucketRegion"] = components.Defaults.ChanField4BucketRegion
	}
	cfg := &components.S3BucketListerConfig{
		Log:                               log,
		Name:                              stepCanonicalName,
		Region:                            sg.Steps[stepName].Data["bucketRegion"],
		BucketName:                        sg.Steps[stepName].Data["bucketName"],
		BucketPrefix:                      sg.Steps[stepName].Data["bucketPrefix"],
		ObjectNamePrefix:                  sg.Steps[stepName].Data["fileNamePrefix"],
		ObjectNameRegexp:                  sg.Steps[stepName].Data["fileNameRegexp"],
		OutputField4BucketName:            sg.Steps[stepName].Data["outputField4BucketName"],
		OutputField4BucketPrefix:          sg.Steps[stepName].Data["outputField4BucketPrefix"],
		OutputField4BucketRegion:          sg.Steps[stepName].Data["outputField4BucketRegion"],
		OutputField4FileName:              sg.Steps[stepName].Data["outputField4FileName"],
		OutputField4FileNameWithoutPrefix: sg.Steps[stepName].Data["outputField4FileNameWithoutPrefix"],
		StepWatcher:                       stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                       sgm.getComponentWaiter(stepName),
		PanicHandlerFn:                    panicHandlerFn}
	// Create and save new OS-files-to-S3 step.
	out, control := componentFunc(cfg)
	sgm.setStepControlChan(stepName, control) // save the control channel.
	sgm.setStepOutputChan(stepName, out)
}

func startSnowflakeLoader(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Setup AUTOCOMMIT.
	deleteAllRows := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["deleteAllRows"])
	// Start step using inputs.
	cfg := &components.SnowflakeLoaderConfig{
		Log:                     log,
		Name:                    stepCanonicalName,
		InputChan:               sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanField4FileName: sg.Steps[stepName].Data["fieldName4FileName"],
		Db:                      sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["logicalConnectionName"]),
		TargetSchemaTableName:   rdbms.SchemaTable{SchemaTable: sg.Steps[stepName].Data["schemaTableName"]},
		StageName:               sg.Steps[stepName].Data["stageName"],
		DeleteAll:               deleteAllRows,
		FnGetSnowflakeSqlSlice:  components.GetSqlSliceSnowflakeCopyInto,
		StepWatcher:             stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:             sgm.getComponentWaiter(stepName),
		PanicHandlerFn:          panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startSnowflakeSync(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Setup AUTOCOMMIT.
	// Start step using inputs.
	cfg := &components.SnowflakeSyncConfig{
		Log:                     log,
		Name:                    stepCanonicalName,
		InputChan:               sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanField4FileName: sg.Steps[stepName].Data["fieldName4FileName"],
		Db:                      sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["logicalConnectionName"]),
		TargetSchemaTableName:   rdbms.SchemaTable{SchemaTable: sg.Steps[stepName].Data["schemaTableName"]},
		StageName:               sg.Steps[stepName].Data["stageName"],
		FlagField:               sg.Steps[stepName].Data["flagFieldName"],
		TargetOtherCols:         helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherCols"]),
		TargetKeyCols:           helper.TokensToOrderedMap(sg.Steps[stepName].Data["keyCols"]),
		// TODO: CommitSequenceKeyName:
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startSnowflakeMerge(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	// Setup AUTOCOMMIT.
	// Start step using inputs.
	cfg := &components.SnowflakeMergeConfig{
		Log:                     log,
		Name:                    stepCanonicalName,
		InputChan:               sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanField4FileName: sg.Steps[stepName].Data["fieldName4FileName"],
		Db:                      sgm.getGlobalTransformManager().getDBConnector(sg.Steps[stepName].Data["logicalConnectionName"]),
		TargetSchemaTableName:   rdbms.SchemaTable{SchemaTable: sg.Steps[stepName].Data["schemaTableName"]},
		StageName:               sg.Steps[stepName].Data["stageName"],
		TargetOtherCols:         helper.TokensToOrderedMap(sg.Steps[stepName].Data["otherCols"]),
		TargetKeyCols:           helper.TokensToOrderedMap(sg.Steps[stepName].Data["keyCols"]),
		// TODO: CommitSequenceKeyName:
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startCSVFileWriter(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	maxFileBytes, err := strconv.Atoi(sg.Steps[stepName].Data["maxFileBytes"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to convert maxFileBytes to integer - check it exists in the pipe config: ", err)
	}
	maxFileRows, err := strconv.Atoi(sg.Steps[stepName].Data["maxFileRows"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to convert maxFileRows to integer - check it exists in the pipe config: ", err)
	}
	useGzip := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["useGzip"])
	appendCreationStamp := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["fileNameSuffixAppendCreationStamp"])
	// Set defaults if missing inputs.
	if sg.Steps[stepName].Data["outputFieldName4FilePath"] == "" {
		sg.Steps[stepName].Data["outputFieldName4FilePath"] = components.Defaults.ChanField4CSVFileName
	}
	// Setup component config.
	cfg := &components.CsvFileWriterConfig{
		Log:                               log,
		Name:                              stepCanonicalName,
		InputChan:                         sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		FileNamePrefix:                    sg.Steps[stepName].Data["fileNamePrefix"],
		FileNameSuffixAppendCreationStamp: appendCreationStamp,
		FileNameSuffixDateFormat:          sg.Steps[stepName].Data["fileNameSuffixDateTimeFormat"],
		FileNameExtension:                 sg.Steps[stepName].Data["fileNameExtension"],
		UseGzip:                           useGzip,
		HeaderFields:                      helper.CsvToStringSliceTrimSpacesRemoveQuotes(sg.Steps[stepName].Data["headerFieldsCSV"]), // TODO: make this use a safe csv reader.
		OutputDir:                         sg.Steps[stepName].Data["outputDir"],
		MaxFileBytes:                      maxFileBytes,
		MaxFileRows:                       maxFileRows,
		OutputChanField4FilePath:          sg.Steps[stepName].Data["outputFieldName4FilePath"],
		StepWatcher:                       stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                       sgm.getComponentWaiter(stepName),
		PanicHandlerFn:                    panicHandlerFn}
	// Create and save new CSV file generator step.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startCopyFilesToS3(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	removeInputFiles := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["removeInputFiles"])
	cfg := &components.CopyFilesToS3Config{
		Log:               log,
		Name:              stepCanonicalName,
		InputChan:         sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		FileNameChanField: sg.Steps[stepName].Data["inputFieldName4FilePath"],
		BucketName:        sg.Steps[stepName].Data["bucketName"],
		BucketPrefix:      sg.Steps[stepName].Data["bucketPrefix"],
		Region:            sg.Steps[stepName].Data["bucketRegion"],
		RemoveInputFiles:  removeInputFiles,
		StepWatcher:       stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:       sgm.getComponentWaiter(stepName),
		PanicHandlerFn:    panicHandlerFn}
	// Create and save new OS-files-to-S3 step.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startChannelBridge(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (inputChan chan chan stream.Record, outputChan chan stream.Record)) {
	cfg := &components.ChannelBridgeConfig{
		Log:            log,
		Name:           stepCanonicalName,
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	in, out := componentFunc(cfg)
	sgm.requestChanInput(stepName, sg.Steps[stepName].Data["readDataFromStep"], in)
	sgm.setStepOutputChan(stepName, out)
	sgm.addBlockingStep(stepName, in)
}

func startChannelCombiner(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	cfg := &components.ChannelCombinerConfig{
		Log:            log,
		Name:           stepCanonicalName,
		Chan1:          sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep1"]),
		Chan2:          sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep2"]),
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	// Create and save new channel combiner step.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep1"])
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep2"])
}

func startManifestWriter(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	inputFieldName4FilePath := components.Defaults.ChanField4CSVFileName
	if sg.Steps[stepName].Data["inputFieldName4FilePath"] != "" {
		inputFieldName4FilePath = sg.Steps[stepName].Data["inputFieldName4FilePath"]
	}
	appendCreationStamp := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["fileNameSuffixAppendCreationStamp"])
	cfg := &components.ManifestWriterConfig{
		Log:                     log,
		Name:                    stepCanonicalName,
		InputChan:               sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanField4FilePath: inputFieldName4FilePath,
		OutputDir:               sg.Steps[stepName].Data["outputDir"],
		ManifestFileNamePrefix:  sg.Steps[stepName].Data["fileNamePrefix"],
		ManifestFileNameSuffixAppendCreationStamp: appendCreationStamp,
		ManifestFileNameSuffixDateFormat:          sg.Steps[stepName].Data["fileNameSuffixDateTimeFormat"],
		ManifestFileNameExtension:                 sg.Steps[stepName].Data["fileNameExtension"],
		OutputChanField4ManifestDir:               sg.Steps[stepName].Data["outputFieldName4ManifestDir"],
		OutputChanField4ManifestName:              sg.Steps[stepName].Data["outputFieldName4ManifestName"],
		OutputChanField4ManifestFullPath:          sg.Steps[stepName].Data["outputFieldName4ManifestFullPath"],
		StepWatcher:                               stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                               sgm.getComponentWaiter(stepName),
		PanicHandlerFn:                            panicHandlerFn}
	// Create and save new manifest writer.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startManifestReader(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	cfg := &components.S3ManifestReaderConfig{
		Log:                          log,
		Name:                         stepCanonicalName,
		InputChan:                    sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanField4ManifestName:  sg.Steps[stepName].Data["inputFieldName4ManifestName"],
		BucketName:                   sg.Steps[stepName].Data["bucketName"],
		BucketPrefix:                 sg.Steps[stepName].Data["bucketPrefix"],
		Region:                       sg.Steps[stepName].Data["bucketRegion"],
		OutputChanField4DataFileName: sg.Steps[stepName].Data["outputFieldName4DataFileName"],
		StepWatcher:                  stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                  sgm.getComponentWaiter(stepName),
		PanicHandlerFn:               panicHandlerFn}
	// Create and save new manifest reader.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startDateRangeGenerator(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	intervalSec, err := strconv.Atoi(sg.Steps[stepName].Data["intervalSeconds"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to fetch intervalSeconds required for DateRangeGenerator: ", err)
	}
	passInputFields, err := strconv.ParseBool(sg.Steps[stepName].Data["passInputFieldsToOutput"])
	if err != nil {
		log.Debug(stepCanonicalName, " unable to parse boolean value found in config field passInputFieldsToOutput: ", err, " - using default false")
		passInputFields = false
	}
	useUTC := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["useUTC"])
	cfg := &components.DateRangeGeneratorConfig{
		Log:                         log,
		Name:                        stepCanonicalName,
		InputChan:                   sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanFieldName4FromDate: sg.Steps[stepName].Data["inputFieldName4FromDate"],
		InputChanFieldName4ToDate:   sg.Steps[stepName].Data["inputFieldName4ToDate"],
		ToDateRFC3339orNow:          sg.Steps[stepName].Data["toDate"],
		UseUTC:                      useUTC,
		IntervalSizeSeconds:         intervalSec,
		OutputChanFieldName4LowDate: sg.Steps[stepName].Data["outputFieldName4LowDate"],
		OutputChanFieldName4HiDate:  sg.Steps[stepName].Data["outputFieldName4HiDate"],
		PassInputFieldsToOutput:     passInputFields,
		StepWatcher:                 stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                 sgm.getComponentWaiter(stepName),
		PanicHandlerFn:              panicHandlerFn}
	// Create and save new date range generator.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startNumberRangeGenerator(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	intervalSizeInt, err := strconv.Atoi(sg.Steps[stepName].Data["intervalSize"])
	intervalSize := float64(intervalSizeInt)
	if err != nil {
		log.Panic(stepCanonicalName, " unable to fetch interval size required for NumberRangeGenerator: ", err)
	}
	leftPadNumZeros, err := strconv.Atoi(sg.Steps[stepName].Data["outputLeftPaddedNumZeros"])
	if err != nil {
		leftPadNumZeros = 0
	}
	passInputFields, err := strconv.ParseBool(sg.Steps[stepName].Data["passInputFieldsToOutput"])
	if err != nil {
		log.Debug(stepCanonicalName, " unable to parse boolean value found in config field passInputFieldsToOutput: ", err, " - using default false")
		passInputFields = false
	}
	cfg := &components.NumberRangeGeneratorConfig{
		Log:                         log,
		Name:                        stepCanonicalName,
		InputChan:                   sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		InputChanFieldName4LowNum:   sg.Steps[stepName].Data["inputFieldName4LowNum"],
		InputChanFieldName4HighNum:  sg.Steps[stepName].Data["inputFieldName4HighNum"],
		IntervalSize:                intervalSize,
		OutputLeftPaddedNumZeros:    leftPadNumZeros,
		OutputChanFieldName4LowNum:  sg.Steps[stepName].Data["outputFieldName4LowNum"],
		OutputChanFieldName4HighNum: sg.Steps[stepName].Data["outputFieldName4HighNum"],
		PassInputFieldsToOutput:     passInputFields,
		StepWatcher:                 stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:                 sgm.getComponentWaiter(stepName),
		PanicHandlerFn:              panicHandlerFn}
	// Create and save new date range generator.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startGenerateRows(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	numRows, err := strconv.Atoi(sg.Steps[stepName].Data["numRows"])
	if err != nil {
		log.Panic(stepCanonicalName, " error: ", err)
	}
	sleepIntervalSec, err := strconv.Atoi(sg.Steps[stepName].Data["sleepIntervalSeconds"])
	if err != nil {
		log.Panic(stepCanonicalName, " unable to read sleepIntervalSeconds required for GenerateRows: ", err)
	}
	cfg := &components.GenerateRowsConfig{
		Log:                    log,
		Name:                   stepCanonicalName,
		MapFieldNamesValuesCSV: sg.Steps[stepName].Data["fieldNamesValuesCSV"],
		FieldName4Sequence:     sg.Steps[stepName].Data["sequenceFieldName"],
		SleepIntervalSeconds:   sleepIntervalSec,
		NumRows:                numRows,
		StepWatcher:            stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:            sgm.getComponentWaiter(stepName),
		PanicHandlerFn:         panicHandlerFn}
	// Create and save new date range generator.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
}

func startFilterRows(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	cfg := &components.FilterRowsConfig{
		Log:            log,
		Name:           stepCanonicalName,
		InputChan:      sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		FilterType:     components.FilterType(sg.Steps[stepName].Data["filterType"]),
		FilterMetadata: components.FilterMetadata(sg.Steps[stepName].Data["filterMetadata"]),
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	// Create and save new manifest reader.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startFieldMapper(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	cfg := &components.FieldMapperConfig{
		Log:            log,
		Name:           stepCanonicalName,
		InputChan:      sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		Steps:          sg.Steps[stepName].ComponentSteps,
		StepWatcher:    stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:    sgm.getComponentWaiter(stepName),
		PanicHandlerFn: panicHandlerFn}
	// Create and save new manifest reader.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}

func startMergeNChannels(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	allowFieldOverwrite := helper.GetTrueFalseStringAsBool(sg.Steps[stepName].Data["readDataFromStep"])
	inputChannels := make([]chan stream.Record, 0)
	for _, val := range sg.Steps[stepName].ComponentSteps { // for each input channel name (stored in ComponentSteps.Type)...
		inputChannels = append(inputChannels, sgm.getStepOutputChan(val.Type)) // save it to supply to component.
		sgm.consumeStep(val.Type)                                              // also save that this step has consumed other channels.
	}
	cfg := &components.MergeNChannelsConfig{
		Log:                 log,
		Name:                stepCanonicalName,
		AllowFieldOverwrite: allowFieldOverwrite,
		InputChannels:       inputChannels,
		StepWatcher:         stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:         sgm.getComponentWaiter(stepName),
		PanicHandlerFn:      panicHandlerFn}
	// Create and save new MergeNChannels.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
}

func startStdOutPassThrough(log logger.Logger,
	stepName string,
	stepCanonicalName string,
	sg *StepGroup,
	sgm StepGroupManager,
	stats StatsManager,
	panicHandlerFn components.PanicHandlerFunc,
	componentFunc func(cfg interface{}) (outputChan chan stream.Record, controlChan chan components.ControlAction)) {
	var i int
	var err error
	if sg.Steps[stepName].Data["abortAfterNumRecords"] == "" {
		i = 0
	} else {
		i, err = strconv.Atoi(sg.Steps[stepName].Data["abortAfterNumRecords"])
		if err != nil {
			log.Panic(stepCanonicalName, " error reading abortAfterNumRecords required for StdOutPassThrough: ", err)
		}
	}
	cfg := &components.StdOutPassThroughConfig{
		Log:             log,
		Name:            stepCanonicalName,
		Writer:          os.Stdout,
		OutputFields:    helper.CsvToStringSliceTrimSpaces2(sg.Steps[stepName].Data["outputFieldsCsv"]),
		AbortAfterCount: int64(i),
		InputChan:       sgm.getStepOutputChan(sg.Steps[stepName].Data["readDataFromStep"]),
		StepWatcher:     stats.AddStepWatcher(stepCanonicalName),
		WaitCounter:     sgm.getComponentWaiter(stepName),
		PanicHandlerFn:  panicHandlerFn}
	// Create and save new manifest reader.
	out, control := componentFunc(cfg)
	sgm.setStepOutputChan(stepName, out)      // save the output channel.
	sgm.setStepControlChan(stepName, control) // save the control channel.
	// Save that this step has consumed other channels.
	sgm.consumeStep(sg.Steps[stepName].Data["readDataFromStep"])
}
