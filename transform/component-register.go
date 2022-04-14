package transform

import (
	"github.com/relloyd/halfpipe/components"
)

// TODO: add error return value from components and handle in launcher functions.
// TODO: add generic launcher function that matches JSON keys to config struct field names sop we can have only one or two launchers.
var componentFuncs = MapComponentFuncs{
	// Type 2 - returns 2 output channels of type stream.StreamRecordIface and ControlAction.
	"SqlExec":                    ComponentRegistration{"2", ComponentRegistrationType2{components.NewSqlExec, startSqlExec}},
	"TableInput":                 ComponentRegistration{"2", ComponentRegistrationType2{components.NewSqlQueryWithArgs, startTableInput}},
	"TableInputWithArgs":         ComponentRegistration{"2", ComponentRegistrationType2{components.NewSqlQueryWithInputChan, startTableInputWithArgs}},
	"TableInputWithReplacements": ComponentRegistration{"2", ComponentRegistrationType2{components.NewSqlQueryWithReplace, startTableInputWithReplacements}},
	"SnowflakeLoader":            ComponentRegistration{"2", ComponentRegistrationType2{components.NewSnowflakeLoader, startSnowflakeLoader}},
	"SnowflakeSync":              ComponentRegistration{"2", ComponentRegistrationType2{components.NewSnowflakeSync, startSnowflakeSync}},
	"SnowflakeMerge":             ComponentRegistration{"2", ComponentRegistrationType2{components.NewSnowflakeMerge, startSnowflakeMerge}},
	"MergeDiff":                  ComponentRegistration{"2", ComponentRegistrationType2{components.NewMergeDiff, startMergeDiff}},
	"TableSync":                  ComponentRegistration{"2", ComponentRegistrationType2{components.NewTableSync, startTableSync}},
	"TableMerge":                 ComponentRegistration{"2", ComponentRegistrationType2{components.NewTableMerge, startTableMerge}},
	"S3BucketList":               ComponentRegistration{"2", ComponentRegistrationType2{components.NewS3BucketList, startS3BucketList}},
	"CSVFileWriter":              ComponentRegistration{"2", ComponentRegistrationType2{components.NewCsvFileWriter, startCSVFileWriter}},
	"CopyFilesToS3":              ComponentRegistration{"2", ComponentRegistrationType2{components.NewCopyFilesToS3, startCopyFilesToS3}},
	"ChannelCombiner":            ComponentRegistration{"2", ComponentRegistrationType2{components.NewChannelCombiner, startChannelCombiner}},
	"ManifestWriter":             ComponentRegistration{"2", ComponentRegistrationType2{components.NewManifestWriter, startManifestWriter}},
	"ManifestReader":             ComponentRegistration{"2", ComponentRegistrationType2{components.NewS3ManifestReader, startManifestReader}},
	"DateRangeGenerator":         ComponentRegistration{"2", ComponentRegistrationType2{components.NewDateRangeGenerator, startDateRangeGenerator}},
	"NumberRangeGenerator":       ComponentRegistration{"2", ComponentRegistrationType2{components.NewNumberRangeGenerator, startNumberRangeGenerator}},
	"GenerateRows":               ComponentRegistration{"2", ComponentRegistrationType2{components.NewGenerateRows, startGenerateRows}},
	"MergeStreamsCartesian":      ComponentRegistration{"2", ComponentRegistrationType2{components.NewMergeNChannels, startMergeNChannels}},
	"StdOutPassThrough":          ComponentRegistration{"2", ComponentRegistrationType2{components.NewStdOutPassThrough, startStdOutPassThrough}},
	"OracleContinuousQueryNotificationToRdbms":     ComponentRegistration{"2", ComponentRegistrationType2{components.NewCqnWithArgs, startTableInputCqnToRdbms}},
	"OracleContinuousQueryNotificationToSnowflake": ComponentRegistration{"2", ComponentRegistrationType2{components.NewCqnWithArgs, startTableInputCqnToSnowflake}},
	// FieldMapper and FilterRows contain their own dynamic features.
	"FieldMapper": ComponentRegistration{"2", ComponentRegistrationType2{components.NewFieldMapper, startFieldMapper}},
	"FilterRows":  ComponentRegistration{"2", ComponentRegistrationType2{components.NewFilterRows, startFilterRows}},
	// Type 3 - returns 1 output chan and 1 input chan of type stream.StreamRecordIface.
	"ChannelBridge": ComponentRegistration{"3", ComponentRegistrationType3{components.NewChannelBridge, startChannelBridge}},
}
