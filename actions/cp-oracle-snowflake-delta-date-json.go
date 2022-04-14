package actions

var jsonOracleSnowflakeDeltaDate = `{
  "schemaVersion": 3,
  "description": "cp delta from Oracle to Snowflake using DATE field",
  "connections": {
    "source": {
      "type": "oracle",
      "logicalName": "${sourceEnv}",
      "data": {
        "dsn": "${sourceDsn}"
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
        "metadataInjectGenerateDateRanges": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "generateDateRanges",
            "readDataFromStep": "getMaxDateInTarget",
            "replaceVariableWithFieldNameCSV": "${maxDateInTarget}:#maxDateInTarget",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "getMaxDateInTarget",
        "metadataInjectGenerateDateRanges"
      ]
    },
    "generateDateRanges": {
      "type": "sequential",
      "steps": {
        "checkLowDate": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select case when min_date > next_batch_start_date then min_date else next_batch_start_date end \"#nextBatchMinDate\", sysdate \"#extractDate\", \"#maxDateAvailable\" from ( select min(${SQLBatchDriverField})-1 min_date /* use min-1 since the extractDateRanges SELECT below uses gt '>' instead of gte '>=' */, to_date('${maxDateInTarget}','YYYYMMDD\"T\"HH24MISS') next_batch_start_date, max(${SQLBatchDriverField}) \"#maxDateAvailable\" from ${sourceTable} )"
          }
        },
        "generateDateRanges": {
          "type": "DateRangeGenerator",
          "data": {
            "readDataFromStep": "checkLowDate",
            "inputFieldName4FromDate": "#nextBatchMinDate",
            "inputFieldName4ToDate": "#maxDateAvailable",
            "useUTC": "false",
            "intervalSeconds": "${SQLBatchSizeSeconds}",
            "outputFieldName4LowDate": "#internalFromDate",
            "outputFieldName4HiDate": "#internalToDate",
			"passInputFieldsToOutput": "true"
          }
        },
        "metadataInjectExtractSourceData": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "extractSourceData",
            "readDataFromStep": "generateDateRanges",
            "replaceVariableWithFieldNameCSV": "${sourceFromDate}:#internalFromDate, ${sourceToDate}:#internalToDate, ${extractDate}:#extractDate",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "checkLowDate",
        "generateDateRanges",
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
            "sqlText": "select ${columnListCsv} from ${sourceTable} where ${SQLBatchDriverField} > to_date('${sourceFromDate}','YYYYMMDD\"T\"HH24MISS') and ${SQLBatchDriverField} <= to_date('${sourceToDate}','YYYYMMDD\"T\"HH24MISS') order by ${SQLBatchDriverField}"
          }
        },
        "csvWriter": {
          "type": "CSVFileWriter",
          "data": {
            "readDataFromStep": "sqlInput",
            "outputDir": "",
            "fileNamePrefix": "${fileNamePrefix}-${sourceFromDate}-${sourceToDate}-${extractDate}",
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
            "fileNamePrefix": "${fileNamePrefix}-${sourceFromDate}-${sourceToDate}-${extractDate}",
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
	"getMaxDateFromTarget"
  ]
}`
