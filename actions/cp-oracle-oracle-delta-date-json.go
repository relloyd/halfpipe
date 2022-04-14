package actions

var jsonOracleOracleDeltaDate = `{
  "schemaVersion": 3,
  "description": "cp delta from Oracle to Oracle using DATE field",
  "connections": {
    "source": {
      "type": "oracle",
      "logicalName": "${sourceLogicalName}",
      "data": {
        "dsn": "${sourceDsn}"
      }
    },
    "target": {
      "type": "oracle",
      "logicalName": "${targetLogicalName}",
      "data": {
        "dsn": "${targetDsn}"
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
            "sqlText": "select nvl(max(${SQLBatchDriverField}), to_date('${SQLBatchStartDateTime}','YYYYMMDD\"T\"HH24MISS')) \"#maxDateInTarget\" from ${targetTable}"
          }
        },
        "metadataInject": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "cpOracleToOracleDelta",
            "readDataFromStep": "getMaxDateInTarget",
            "replaceVariableWithFieldNameCSV": "${sourceFromDate}:#maxDateInTarget",
            "replaceDateTimeUsingFormat": "20060102T150405"
          }
        }
      },
      "sequence": [
        "getMaxDateInTarget",
        "metadataInject"
      ]
    },
    "cpOracleToOracleDelta": {
      "type": "sequential",
      "steps": {
        "readFromSource": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select ${columnListCsv} from ${sourceTable} where ${SQLBatchDriverField} > to_date('${sourceFromDate}','YYYYMMDD\"T\"HH24MISS') order by ${SQLBatchDriverField}"
          }
        },
        "writeToTarget": {
          "type": "TableMerge",
          "data": {
            "readDataFromStep": "readFromSource",
            "databaseConnectionName": "target",
            "outputSchemaName": "${targetSchema}",
            "outputTable": "${targetTable}",
            "commitBatchSize": "${targetCommitBatchSize}",
            "execBatchSize": "${targetExecBatchSize}",
            "keyCols": "${targetKeyColumns}",
            "otherCols": "${targetOtherColumns}"
          }
        }
      },
      "sequence": [
        "readFromSource",
        "writeToTarget"
      ]
    }
  },
  "sequence": [
    "getMaxDateFromTarget"
  ]
}
`
