package actions

type SyncConfig struct {
	// Connections
	SrcAndTgtConnections
	// Generic
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	StatsDumpFrequencySeconds int
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	// Sync Specific
	SQLPrimaryKeyFieldsCsv             string
	SQLTargetTableOriginRowIdFieldName string
	CommitBatchSize                    int
	TxtBatchNumRows                    int
	// Snowflake Specific
	SnowTableName     string `errorTxt:"Snowflake [schema.]table" mandatory:"yes"`
	SnowStageName     string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix string `errorTxt:"csv file name prefix" mandatory:"yes"`
	CsvHeaderFields   string `errorTxt:"csv header fields"`
	CsvMaxFileRows    int    `errorTxt:"csv max file rows"`
	CsvMaxFileBytes   int    `errorTxt:"csv max file bytes"`
}
