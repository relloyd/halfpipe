package actions

var jsonOracleOracleDeltaNumber = `{
  "schemaVersion": 3,
  "description": "cp delta from Oracle to Oracle using NUMBER field",
  "connections": {
    "source": {
      "type": "oracle",
      "logicalName": "${srcLogicalName}",
      "data": {
        "host": "${srcOracleHostname}",
        "databaseName": "${srcOracleDatabaseName}",
        "port": "${srcOraclePort}",
        "userName": "${srcOracleUserName}",
        "password": "${srcOraclePassword}",
        "parameters": "${srcOracleParameters}"
      }
    },
    "target": {
      "type": "oracle",
      "logicalName": "${tgtLogicalName}",
      "data": {
        "host": "${tgtOracleHostname}",
        "databaseName": "${tgtOracleDatabaseName}",
        "port": "${tgtOraclePort}",
        "userName": "${tgtOracleUserName}",
        "password": "${tgtOraclePassword}",
        "parameters": "${tgtOracleParameters}"
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
            "sqlText": "select nvl(max(${SQLBatchDriverField}), ${SQLBatchStartSequence}) \"#maxSequenceInTarget\" from ${targetTable}"
          }
        },
        "metadataInject": {
          "type": "MetadataInjection",
          "data": {
            "executeTransformName": "cpOracleToOracleDelta",
            "readDataFromStep": "getMaxSequenceInTarget",
            "replaceVariableWithFieldNameCSV": "${sourceFromSequence}:#maxSequenceInTarget"
          }
        }
      },
      "sequence": [
        "getMaxSequenceInTarget",
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
            "sqlText": "select ${columnListCsv} from ${sourceTable} where ${SQLBatchDriverField} > ${sourceFromSequence} order by ${SQLBatchDriverField}"
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
    "getMaxSequenceFromTarget"
  ]
}
`
