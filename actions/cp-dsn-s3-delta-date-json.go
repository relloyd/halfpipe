package actions

var jsonDsnS3DeltaDate = `{
  "schemaVersion": 3,
  "description": "cp delta from DSN to S3 using DATE drvier field",
  "connections": {
    "source": {
      "type": "${srcType}",
      "logicalName": "${srcLogicalName}",
      "data": {
        "dsn": "${srcDsn}"
      }
    },
    "target": {
      "type": "s3",
      "logicalName": "${tgtLogicalName}",
      "data": {
        "name": "${tgtS3BucketName}",
        "prefix": "${tgtS3BucketPrefix}",
        "region": "${tgtS3Region}"
      }
    }
  },
  "type": "${repeatTransform}",
  "repeatMetadata": {
    "sleepSeconds": ${sleepSeconds}
  },
  "transformGroups": {
    "getMaxDateFromS3": {
      "type": "sequential",
      "steps": {
        "getMaxDateInTarget": {
          "type": "S3BucketList",
          "data": {
            "bucketRegion": "${tgtS3Region}",
            "bucketName": "${tgtS3BucketName}",
            "bucketPrefix": "${tgtS3BucketPrefix}",
            "fileNamePrefix": "${fileNamePrefix}",
            "fileNameRegexp": ".+-[0-9]{8}T[0-9]{6}-([0-9]{8}T[0-9]{6})-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.man",
            "outputField4BucketName": "#bucketName",
            "outputField4BucketPrefix": "#bucketPrefix",
            "outputField4BucketRegion": "#bucketRegion",
            "outputField4FileName": "#fileName"
          }
        },
        "fieldMapper": {
          "type": "FieldMapper",
          "data": {
            "readDataFromStep": "getMaxDateInTarget"
          },
          "steps": [
            {
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#fileName",
                "regexpMatch": ".+-[0-9]{8}T[0-9]{6}-([0-9]{8}T[0-9]{6})-[0-9]{8}T[0-9]{6}_[0-9]{6}\\.man",
                "regexpReplace": "$1",
                "resultField": "#maxS3FileDate"
              }
            }
          ]
        },
		"filterRows": {
          "type": "FilterRows",
          "data": {
            "readDataFromStep": "fieldMapper",
            "filterType": "GetMax",
            "filterMetadata": "#maxS3FileDate"
          }
        },
        "dummyRow": {
          "type": "GenerateRows",
          "data": {
            "fieldNamesValuesCSV": "#maxS3FileDate:19000101T000000",
            "sequenceFieldName": "#sequence",
            "numRows": "1",
            "sleepIntervalSeconds": "0"
          }
        },
        "joinStreams": {
          "type": "ChannelCombiner",
          "data": {
            "readDataFromStep1": "dummyRow",
            "readDataFromStep2": "filterRows"
          }
        },
        "filterRows2": {
          "type": "FilterRows",
          "data": {
            "readDataFromStep": "joinStreams",
            "filterType": "GetMax",
            "filterMetadata": "#maxS3FileDate"
          }
        },
        "metadataInjectGenerateDateRanges": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "generateDateRanges",
            "readDataFromStep": "filterRows2",
            "replaceVariableWithFieldNameCSV": "${maxDateInTarget}:#maxS3FileDate",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "getMaxDateInTarget",
        "fieldMapper",
        "filterRows",
        "dummyRow",
        "joinStreams",
        "filterRows2",
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
            "sqlText": "select case when min_date > next_batch_start_date then min_date else next_batch_start_date end \"#nextBatchMinDate\", ${currentDateTime} \"#extractDate\", \"#maxDateAvailable\" from ( select dateadd(day, -1, min(${SQLBatchDriverField})) min_date /* use min-1 since the extractDateRanges SELECT below uses gt '>' instead of gte '>=' */, ${deltaDateTemplateLowDate} next_batch_start_date, max(${SQLBatchDriverField}) \"#maxDateAvailable\" from ${sourceTable} ) a"
          }
        },
        "generateDateRanges": {
          "type": "DateRangeGenerator",
          "data": {
            "readDataFromStep": "checkLowDate",
            "inputFieldName4FromDate": "#nextBatchMinDate",
            "inputFieldName4ToDate": "#maxDateAvailable",
            "toDate": "now",
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
            "sqlText": "select ${columnListCsv} from ${sourceTable} where ${SQLBatchDriverField} > ${deltaDateTemplateSourceFromDate} and ${SQLBatchDriverField} <= ${deltaDateTemplateSourceToDate} order by ${SQLBatchDriverField}"
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
        }
      },
      "sequence": [
        "sqlInput",
        "csvWriter",
        "copyFilesToS3",
        "manifestWriter",
        "copyManifestToS3"
      ]
    }
  },
  "sequence": [
    "getMaxDateFromS3"
  ]
}`
