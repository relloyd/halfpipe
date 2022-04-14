package actions

var jsonOracleSnowflakeDeltaNumber = `{
  "schemaVersion": 3,
  "description": "cp delta from Oracle to Snowflake using NUMBER field",
  "connections": {
    "source": {
      "type": "oracle",
      "logicalName": "${sourceEnv}",
      "data": {
        "host": "${oracleHostname}",
        "databaseName": "${oracleDatabaseName}",
        "port": "${oraclePort}",
        "userName": "${oracleUserName}",
        "password": "${oraclePassword}",
        "parameters": "${oracleParameters}"
      }
    },
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
    "getMaxSequenceFromTarget": {
      "type": "sequential",
      "steps": {
        "getMaxSequenceInTarget": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "target",
            "sqlText": "select nvl(max(${SQLBatchDriverField}), ${SQLBatchStartSequence}) \"#maxSequenceInTarget\" from ${snowflakeTable}"
          }
        },
        "metadataInject": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "generateNumberRanges",
            "readDataFromStep": "getMaxSequenceInTarget",
            "replaceVariableWithFieldNameCSV": "${maxSequenceInTarget}:#maxSequenceInTarget",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "getMaxSequenceInTarget",
        "metadataInject"
      ]
    },
    "generateNumberRanges": {
      "type": "sequential",
      "steps": {
        "checkLowSequence": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select case when min_sequence > next_batch_start_sequence then min_sequence else next_batch_start_sequence end \"#nextBatchMinSequence\", \"#maxSequenceInTarget\", sysdate \"#extractDate\" from ( select min(${SQLBatchDriverField})-1 min_sequence /* use min-1 since the extractSequenceRanges SELECT below uses gt '>' instead of gte '>=' */, ${maxSequenceInTarget} next_batch_start_sequence, max(${SQLBatchDriverField}) \"#maxSequenceInTarget\" from ${sourceTable} )"
          }
        },
        "generateNumberRangesStep": {
          "type": "NumberRangeGenerator",
          "data": {
            "readDataFromStep": "checkLowSequence",
            "inputFieldName4LowNum": "#nextBatchMinSequence",
            "inputFieldName4HighNum": "#maxSequenceInTarget",
            "intervalSize": "${SQLBatchSize}",
			"outputLeftPaddedNumZeros": "12",
            "outputFieldName4LowNum": "#internalLowNum",
            "outputFieldName4HighNum": "#internalHighNum",
			"passInputFieldsToOutput": "true"
          }
        },
        "metadataInjectExtractSourceData": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "extractSourceData",
            "readDataFromStep": "generateNumberRangesStep",
            "replaceVariableWithFieldNameCSV": "${sourceFromSequence}:#internalLowNum, ${sourceToSequence}:#internalHighNum, ${extractDate}:#extractDate",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "checkLowSequence",
        "generateNumberRangesStep",
        "metadataInjectExtractSourceData"
      ]
    },
    "extractSourceData": {
      "type": "sequential",
      "steps": {
        "sqlInput": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select ${columnListCsv} from ${sourceTable} where ${SQLBatchDriverField} > ${sourceFromSequence} and ${SQLBatchDriverField} <= ${sourceToSequence} order by ${SQLBatchDriverField}"
          }
        },
        "csvWriter": {
          "type": "CSVFileWriter",
          "data": {
            "readDataFromStep": "sqlInput",
            "outputDir": "",
            "fileNamePrefix": "${fileNamePrefix}-${sourceFromSequence}-${sourceToSequence}-${extractDate}",
            "fileNameSuffixAppendCreationStamp": "false",
            "fileNameSuffixDateTimeFormat": "20060102T150405",
            "fileNameExtension": "csv",
            "useGzip": "true",
            "headerFieldsCSV": "${csvHeaderFields}",
            "maxFileRows": "${csvMaxFileRows}",
            "maxFileBytes": "${csvMaxFileBytes}",
            "outputFieldName4FilePath": "#internalFilePath"
          }
        },
        "copyFilesToS3": {
          "type": "CopyFilesToS3",
          "data": {
            "readDataFromStep": "csvWriter",
            "inputFieldName4FilePath": "#internalFilePath",
            "bucketName": "${tgtS3BucketName}",
            "bucketPrefix": "${tgtS3BucketPrefix}",
            "bucketRegion": "${tgtS3Region}",
            "removeInputFiles": "true"
          }
        },
        "manifestWriter": {
          "type": "ManifestWriter",
          "data": {
            "readDataFromStep": "copyFilesToS3",
            "inputFieldName4FilePath": "#internalFilePath",
            "outputDir": "",
            "fileNamePrefix": "${fileNamePrefix}-${sourceFromSequence}-${sourceToSequence}-${extractDate}",
            "fileNameSuffixAppendCreationStamp": "false",
            "fileNameSuffixDateTimeFormat": "20060102T150405",
            "fileNameExtension": "man",
            "outputFieldName4ManifestDir": "#manifestDir",
            "outputFieldName4ManifestName": "#manifestFile",
            "outputFieldName4ManifestFullPath": "#manifestFullPath"
          }
        },
        "copyManifestToS3": {
          "type": "CopyFilesToS3",
          "data": {
            "readDataFromStep": "manifestWriter",
            "inputFieldName4FilePath": "#manifestFullPath",
            "bucketName": "${tgtS3BucketName}",
            "bucketPrefix": "${tgtS3BucketPrefix}",
            "bucketRegion": "${tgtS3Region}",
            "removeInputFiles": "true"
          }
        },
        "manifestReader": {
          "type": "ManifestReader",
          "data": {
            "readDataFromStep": "copyManifestToS3",
            "inputFieldName4ManifestName": "#manifestFile",
            "bucketName": "${tgtS3BucketName}",
            "bucketPrefix": "${tgtS3BucketPrefix}",
            "bucketRegion": "${tgtS3Region}",
            "outputFieldName4DataFileName": "#dataFile"
          }
        },
        "copyIntoSnowflake": {
          "type": "SnowflakeMerge",
          "data": {
            "readDataFromStep": "manifestReader",
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
        "sqlInput",
        "csvWriter",
        "copyFilesToS3",
        "manifestWriter",
        "copyManifestToS3",
        "manifestReader",
        "copyIntoSnowflake"
      ]
    }
  },
  "sequence": [
	"getMaxSequenceFromTarget"
  ]
}`
