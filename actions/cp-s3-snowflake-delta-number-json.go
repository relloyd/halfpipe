package actions

var jsonS3SnowflakeDeltaNumber = `{
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
    "getMaxFromTarget": {
      "type": "sequential",
      "steps": {
        "getMaxSequenceInTarget": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "target",
			"sqlText": "select nvl(max(${SQLBatchDriverField}), ${SQLBatchStartSequence}) \"#maxSequenceInTarget\" from ${snowflakeTable}"
          }
        },
        "metadataInjectLoadData": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "loadData",
            "readDataFromStep": "getMaxSequenceInTarget",
            "replaceVariableWithFieldNameCSV": "${maxSequenceInTarget}:#maxSequenceInTarget",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "getMaxSequenceInTarget",
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
            "fileNameRegexp": ".+-[0-9]{12}-[0-9]{12}-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.csv.*",
            "outputField4BucketName": "#bucketName",
            "outputField4BucketPrefix": "#bucketPrefix",
            "outputField4BucketRegion": "#bucketRegion",
            "outputField4FileName": "#dataFilePath",
            "outputField4FileNameWithoutPrefix": "#dataFile"
          }
        },
		"fieldMapperGetS3FileToSequence": {
          "type": "FieldMapper",
          "data": {
            "readDataFromStep": "getS3Files"
          },
          "steps": [
            {
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#dataFile",
                "regexpMatch": ".+-([0-9]{12})-[0-9]{12}-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.csv.*",
                "regexpReplace": "$1",
                "resultField": "#S3FileFromSequence"
              }
            }
          ]
        },
		"filterGreaterThan": {
          "type": "FilterRows",
          "data": {
            "readDataFromStep": "fieldMapperGetS3FileToSequence",
            "filterType": "JsonLogic",
            "filterMetadata": "{ \">\" : [ { \"var\" : \"#S3FileFromSequence\" }, \"${maxSequenceInTarget}\" ] }"
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
        "fieldMapperGetS3FileToSequence",
        "filterGreaterThan",
        "copyIntoSnowflake"
      ]
    }
  },
  "sequence": [
	"getMaxFromTarget"
  ]
}`
