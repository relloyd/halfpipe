package actions

type CpConfig struct {
	// Connections
	SrcAndTgtConnections
	// Generic
	LogLevel                  string `errorTxt:"log level" mandatory:"yes"`
	StackDumpOnPanic          bool
	StatsDumpFrequencySeconds int
	RepeatInterval            int `errorTxt:"repeat interval"`
	ExportConfigType          string
	ExportIncludeConnections  bool
	// Snowflake common
	SnowStageName     string `errorTxt:"Snowflake stage" mandatory:"yes"`
	BucketRegion      string `errorTxt:"s3 region" mandatory:"yes"`
	BucketName        string `errorTxt:"s3 bucket" mandatory:"yes"`
	BucketPrefix      string `errorTxt:"s3 prefix"`
	CsvFileNamePrefix string `errorTxt:"csv file name prefix" mandatory:"yes"`
	CsvRegexp         string `errorTxt:"csv file regexp filter"`
	CsvHeaderFields   string `errorTxt:"csv header fields"`
	CsvMaxFileRows    string `errorTxt:"csv max file rows"`
	CsvMaxFileBytes   string `errorTxt:"csv max file bytes"`
	// Delta action specific
	SQLBatchDriverField    string `errorTxt:"source driver field" mandatory:"yes"`
	SQLBatchStartSequence  int    `errorTxt:"SQL batch start sequence (number)" mandatory:"yes"`
	SQLBatchStartDateTime  string `errorTxt:"SQL batch start date-time" mandatory:"yes"`
	SQLBatchSizeSeconds    string `errorTxt:"SQL batch size seconds"`
	SQLBatchSize           string `errorTxt:"SQL batch size days"`
	SQLPrimaryKeyFieldsCsv string `errorTxt:"primary key fields CSV"`
	// Metadata action specific
	ExecuteDDL bool
	// Oracle-Oracle action specific
	CommitBatchSize string
	ExecBatchSize   string
	// Target specific
	AppendTarget bool
}
