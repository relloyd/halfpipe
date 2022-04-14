package actions

var jsonS3SnowflakeDeltaDate = `{
  "schemaVersion": 3,
  "description": "cp delta from S3 to Snowflake using DATE field",
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
    "getMaxDateFromTarget": {
      "type": "sequential",
      "steps": {
        "getMaxDateInTarget": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "target",
            "sqlText": "select nvl(max(${SQLBatchDriverField}), to_date('${SQLBatchStartDateTime}','YYYYMMDD\"T\"HH24MISS')) \"#maxDateInTarget\" from ${snowflakeTable}"
          }
        },
        "metadataInjectLoadData": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "loadData",
            "readDataFromStep": "getMaxDateInTarget",
            "replaceVariableWithFieldNameCSV": "${maxDateInTarget}:#maxDateInTarget",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "getMaxDateInTarget",
        "metadataInjectLoadData"
      ]
    },
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
            "fileNameRegexp": ".+-[0-9]{8}T[0-9]{6}-[0-9]{8}T[0-9]{6}-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.csv.*",
            "outputField4BucketName": "#bucketName",
            "outputField4BucketPrefix": "#bucketPrefix",
            "outputField4BucketRegion": "#bucketRegion",
            "outputField4FileName": "#dataFilePath",
            "outputField4FileNameWithoutPrefix": "#dataFile"
          }
        },
		"fieldMapperGetS3FileToDate": {
          "type": "FieldMapper",
          "data": {
            "readDataFromStep": "getS3Files"
          },
          "steps": [
            {
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#dataFile",
                "regexpMatch": ".+-([0-9]{8}T[0-9]{6})-[0-9]{8}T[0-9]{6}-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.csv.*",
                "regexpReplace": "$1",
                "resultField": "#S3FileFromDate"
              }
            }
          ]
        },
		"filterGreaterThan": {
          "type": "FilterRows",
          "data": {
            "readDataFromStep": "fieldMapperGetS3FileToDate",
            "filterType": "JsonLogic",
            "filterMetadata": "{ \">\" : [ { \"var\" : \"#S3FileFromDate\" }, \"${maxDateInTarget}\" ] }"
          }
		},
        "copyIntoSnowflake": {
          "type": "SnowflakeMerge",
          "data": {
            "readDataFromStep": "filterGreaterThan",
            "logicalConnectionName": "target",
            "fieldName4FileName": "#dataFile",
            "stageName": "${snowflakeStage}",
            "schemaTableName": "${snowflakeTable}",
			"keyCols": "${snowflakeTargetKeyColumns}",
            "otherCols": "${snowflakeTargetOtherColumns}"
          }
        }
      },
      "sequence": [
        "getS3Files",
        "fieldMapperGetS3FileToDate",
        "filterGreaterThan",
        "copyIntoSnowflake"
      ]
    }
  },
  "sequence": [
	"getMaxDateFromTarget"
  ]
}`
