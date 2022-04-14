package constants

// Component

const (
	MergeDiffValueNew                     = "N"
	MergeDiffValueChanged                 = "C"
	MergeDiffValueDeleted                 = "D"
	MergeDiffValueIdentical               = "I"
	DiffStatusFieldName                   = "#diffStatus"
	ChanSize                              = 20000
	StatsCaptureFrequencySeconds          = 5
	TimeFormatYearSeconds                 = "20060102T150405" // used for human readable file names
	TimeFormatYearSecondsRegex            = "[0-9]{4}[0-9]{2}[0-9]{2}T[0-9]{6}"
	TimeFormatYearSecondsTZ               = "20060102T150405-0700" // a format that includes the time zone and is compatible with Oracle and Snowflake databases.
	ManifestHeaderColumnName              = "FileName"
	CqnFlagFieldName                      = "#cqnMergeDiffFlag"
	TableSyncDefaultCommitSequenceKeyName = "#commitSequence"
	TableSyncBatchSizeDefault             = 1000
	TableSyncTxtBatchNumRowsDefault       = 10
	OracleConnectionDefaultParams         = "prefetch_rows=500"
	EmojiBang                             = "\U0001F4A5"
	EnvVarPrefix                          = "HP" // prefixed for environment variables in twelveFactorMode
	HpPluginOracle                        = "hp-oracle-plugin.so"
	HpPluginOdbc                          = "hp-odbc-plugin.so"
	EnvVarPluginDir                       = EnvVarPrefix + "_PLUGIN_DIR"
	EnvVarAuthzDevMode                    = EnvVarPrefix + "_AUTH_MODE_DEV"
	EnvVarAuthzAmiMode                    = EnvVarPrefix + "_AUTH_MODE_AWS_AMI"
	EnvVarAuthzAmiModeDebug               = EnvVarPrefix + "_AUTH_MODE_AWS_AMI_WITH_D3BUG"
	ActionFuncsCommandCp                  = "cp"
	ActionFuncsCommandSync                = "sync"
	ActionFuncsSubCommandMeta             = "meta"
	ActionFuncsSubCommandDelta            = "delta"
	ActionFuncsSubCommandSnapshot         = "snap"
	ActionFuncsSubCommandEvents           = "events"
	ActionFuncsSubCommandBatch            = "batch"
	ConnectionTypeStdout                  = "stdout"
	ConnectionTypeOracle                  = "oracle"
	ConnectionTypeMockOracle              = "mockOracle"
	ConnectionTypeSnowflake               = "snowflake"
	ConnectionTypeNetezza                 = "netezza"
	ConnectionTypeOdbc                    = "odbc" // this is not a real connection type, since we need a suffix to provide the driver name like sqlserver.
	ConnectionTypeOdbcSqlServer           = "odbc+sqlserver"
	ConnectionTypeSqlServer               = "sqlserver"
	ConnectionTypeS3                      = "s3"
)

// EmojiSmile = "\U0001F604"
// EmojiSnowboader = "\U0001F3C2"
