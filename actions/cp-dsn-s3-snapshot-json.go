package actions

var jsonOdbcS3Snapshot = `{
  "schemaVersion": 3,
  "description": "cp snapshot from DSN database to S3",
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
    "cpDsnToS3": {
      "type": "sequential",
      "steps": {
        "readFromSource": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select ${columnListCsv} from ${sourceTable}"
          }
        },
        "csvWriter": {
          "type": "CSVFileWriter",
          "data": {
            "readDataFromStep": "readFromSource",
            "outputDir": "",
            "fileNamePrefix": "${fileNamePrefix}",
            "fileNameSuffixAppendCreationStamp": "true",
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
            "bucketName": "${tgtS3BucketName}",
            "bucketPrefix": "${tgtS3BucketPrefix}",
            "bucketRegion": "${tgtS3Region}",
            "removeInputFiles": "true"
          }
        }
      },
      "sequence": [
        "readFromSource",
        "csvWriter",
        "copyFilesToS3",
        "manifestWriter",
        "copyManifestToS3"
      ]
    }
  },
  "sequence": [
    "cpDsnToS3"
  ]
}`
