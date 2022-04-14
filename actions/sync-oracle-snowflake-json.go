package actions

var jsonSyncOracleSnowflake = `{
  "schemaVersion": 3,
  "description": "sync batch from Oracle to Snowflake",
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
    "syncTransform": {
      "type": "sequential",
      "steps": {
        "getFromSource": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select ${columnListCsv} from ${sourceTable} order by ${SQLPrimaryKeyFieldsCsv}"
          }
        },
        "getFromTarget": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "target",
            "sqlText": "select ${columnListCsv} from ${snowflakeSchemaTable} order by ${SQLPrimaryKeyFieldsCsv}"
          }
        },
        "diff": {
          "type": "MergeDiff",
          "data": {
            "readOldDataFromStep": "getFromTarget",
            "readNewDataFromStep": "getFromSource",
            "joinKeys": "${keyTokens}",
            "compareKeys": "${otherTokens}",
            "flagFieldName": "#flagField",
            "outputIdenticalRows": "false"
          }
        },
        "csvWriter": {
          "type": "CSVFileWriter",
          "data": {
            "readDataFromStep": "diff",
            "outputDir": "",
            "fileNamePrefix": "${fileNamePrefix}",
            "fileNameSuffixAppendCreationStamp": "true",
            "fileNameSuffixDateTimeFormat": "20060102T150405",
            "fileNameExtension": "csv",
            "useGzip": "true",
            "headerFieldsCSV": "${csvHeaderFields},\"#flagField\"",
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
            "bucketName": "${bucketName}",
            "bucketPrefix": "${bucketPrefix}",
            "bucketRegion": "${bucketRegion}",
            "removeInputFiles": "true"
          }
        },
        "manifestWriter": {
          "type": "ManifestWriter",
          "data": {
            "readDataFromStep": "copyFilesToS3",
            "inputFieldName4FilePath": "#internalFilePath",
            "outputDir": "",
            "fileNamePrefix": "${fileNamePrefix}",
            "fileNameSuffixAppendCreationStamp": "true",
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
            "bucketName": "${bucketName}",
            "bucketPrefix": "${bucketPrefix}",
            "bucketRegion": "${bucketRegion}",
            "removeInputFiles": "true"
          }
        },
        "manifestReader": {
          "type": "ManifestReader",
          "data": {
            "readDataFromStep": "copyManifestToS3",
            "inputFieldName4ManifestName": "#manifestFile",
            "bucketName": "${bucketName}",
            "bucketPrefix": "${bucketPrefix}",
            "bucketRegion": "${bucketRegion}",
            "outputFieldName4DataFileName": "#dataFile"
          }
        },
        "syncTable": {
          "type": "SnowflakeSync",
          "data": {
            "readDataFromStep": "manifestReader",
            "logicalConnectionName": "target",
            "fieldName4FileName": "#dataFile",
            "use1Transaction": "true",
            "stageName": "${snowflakeStage}",
            "schemaTableName": "${snowflakeSchemaTable}",
            "keyCols": "${keyTokens}",
            "otherCols": "${otherTokens}",
            "flagFieldName": "#flagField",
            "commitSequenceKeyName": "#syncCommitSequence"
          }
        }
      },
      "sequence": [
        "getFromSource",
        "getFromTarget",
        "diff",
        "csvWriter",
        "copyFilesToS3",
        "manifestWriter",
        "copyManifestToS3",
        "manifestReader",
        "syncTable"
      ]
    }
  },
  "sequence": [
    "syncTransform"
  ]
}`
