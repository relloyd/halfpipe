package actions

var jsonDsnSnapshot = `{
  "schemaVersion": 3,
  "description": "cp snapshot from DSN to DSN",
  "connections": {
    "source": {
      "type": "${srcType}",
      "logicalName": "${srcLogicalName}",
      "data": {
        "dsn": "${srcDsn}"
      }
    },
    "target": {
      "type": "${tgtType}",
      "logicalName": "${tgtLogicalName}",
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
    "optionalTruncateTarget": {
      "type": "sequential",
      "steps": {
        "generateRows": {
          "type": "GenerateRows",
          "data": {
            "fieldNamesValuesCSV": "\"#sqlText:truncate table ${targetTable}\"",
            "numRows": "${truncateTargetEnabled1orDisabled0}",
            "sleepIntervalSeconds": "0"
          }
        },
        "truncateTable": {
          "type": "SqlExec",
          "data": {
            "readDataFromStep": "generateRows",
            "databaseConnectionName": "target",
            "sqlQueryFieldName": "#sqlText"
          }
        }
      },
      "sequence": [
        "generateRows",
        "truncateTable"
      ]
    },
    "cpDsn": {
      "type": "sequential",
      "steps": {
        "readFromSource": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select ${columnListCsv}, '${MergeDiffValueNew}' as \"#flagField\" from ${sourceTable}"
          }
        },
        "writeToTarget": {
          "type": "TableSync",
          "data": {
            "readDataFromStep": "readFromSource",
            "databaseConnectionName": "target",
            "outputSchemaName": "${targetSchema}",
            "outputTable": "${targetTable}",
            "flagFieldName": "#flagField",
            "commitBatchSize": "${targetBatchSize}",
            "txtBatchNumRows": "1000",
			"keyCols": "${targetKeyColumns}",
            "otherCols": ""
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
    "optionalTruncateTarget",
    "cpDsn"
  ]
}
`
