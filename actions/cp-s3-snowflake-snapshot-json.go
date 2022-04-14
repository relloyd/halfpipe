package actions

var jsonS3SnowflakeSnap = `{
  "schemaVersion": 3,
  "description": "cp snapshot from S3 to Snowflake",
  "connections": {
    "target": {
      "type": "snowflake",
      "logicalName": "${targetEnv}",
      "data": {
        "dsn": "${tgtDsn}"
      }
    }
  },
  "type": "${repeatTransform}",
  "repeatMetadata": {
	"sleepSeconds": ${sleepSeconds}
  },
  "transformGroups": {
    "loadData": {
      "type": "sequential",
      "steps": {
        "getS3Files": {
          "type": "S3BucketList",
          "data": {
            "bucketRegion": "${tgtS3Region}",
            "bucketName": "${tgtS3BucketName}",
            "bucketPrefix": "${tgtS3BucketPrefix}",
            "fileNamePrefix": "${fileNamePrefix}",
            "fileNameRegexp": "${fileNameRegexp}",
            "outputField4BucketName": "#bucketName",
            "outputField4BucketPrefix": "#bucketPrefix",
            "outputField4BucketRegion": "#bucketRegion",
            "outputField4FileName": "#dataFilePath",
            "outputField4FileNameWithoutPrefix": "#dataFile"
          }
        },
        "copyIntoSnowflake": {
          "type": "SnowflakeLoader",
          "data": {
            "logicalConnectionName": "target",
            "fieldName4FileName": "#dataFile",
			"use1Transaction": "true",
			"deleteAllRows": "${deleteTarget}",
            "readDataFromStep": "getS3Files",
            "stageName": "${snowflakeStage}",
            "schemaTableName": "${snowflakeTable}"
          }
        }
      },
      "sequence": [
        "getS3Files",
        "copyIntoSnowflake"
      ]
    }
  },
  "sequence": [
	"loadData"
  ]
}`
