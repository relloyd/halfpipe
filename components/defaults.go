package components

// Default field names are used by components to know the names of input and output fields.
var Defaults = struct {
	ChanField4CSVFileName           string
	ChanField4FileName              string // the default map key that contains the file names found in the S3 bucket, used by input and output Channels.
	ChanField4FileNameWithoutPrefix string // the default map key that contains the file names found in the S3 bucket, used by input and output Channels.
	ChanField4BucketName            string // the default map key that contains the bucket name, used by input and output Channels.
	ChanField4BucketPrefix          string // the default map key that contains the bucket prefix, used by input and output Channels.
	ChanField4BucketRegion          string // the default map key that contains the bucket region, used by input and output Channels.
	ChanField4StageName             string // the default map key that contains the Snowflake stage name, used by input and output Channels.
	ChanField4TableName             string // the default map key that contains the Snowflake table name, used by input and output Channels.
}{
	ChanField4CSVFileName:           "#CSVFileName",
	ChanField4FileName:              "#DataFileName",
	ChanField4FileNameWithoutPrefix: "#DataFileNameWithoutPrefix",
	ChanField4BucketName:            "#BucketName",
	ChanField4BucketPrefix:          "#BucketPrefix",
	ChanField4BucketRegion:          "#BucketRegion",
	ChanField4StageName:             "#SnowflakeStageName",
	ChanField4TableName:             "#SnowflakeTargetTableName",
}
