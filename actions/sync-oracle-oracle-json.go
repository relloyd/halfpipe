package actions

var jsonSyncOracleOracle = `{
  "schemaVersion": 3,
  "description": "sync batch from Oracle to Oracle",
  "connections": {
    "source": {
      "type": "oracle",
      "logicalName": "${sourceEnv}",
      "data": {
        "dsn": "${sourceDsn}"
      }
    },
    "target": {
      "type": "oracle",
      "logicalName": "${targetEnv}",
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
    "syncTransformName": {
	  "type": "sequential",
      "steps": {
        "getFromSource": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "source",
            "sqlText": "select ${columnListCsv} from ${sourceTable} order by ${SQLPrimaryKeyFieldsCsv}"
          }
        },
        "getFromCopy": {
          "type": "TableInput",
          "data": {
            "databaseConnectionName": "target",
            "sqlText": "select ${columnListCsv} from ${targetTable} order by ${SQLPrimaryKeyFieldsCsv}"
          }
        },
        "diff": {
          "type": "MergeDiff",
          "data": {
            "readOldDataFromStep": "getFromCopy",
            "readNewDataFromStep": "getFromSource",
            "joinKeys": "${keyTokens}",
            "compareKeys": "${otherTokens}",
            "flagFieldName": "#flagField",
            "outputIdenticalRows": "false"
          }
        },
        "sync": {
          "type": "TableSync",
          "data": {
            "readDataFromStep": "diff",
            "databaseConnectionName": "target",
            "outputSchemaName": "${syncTargetSchema}",
            "outputTable": "${syncTargetTable}",
            "flagFieldName": "#flagField",
            "commitBatchSize": "${targetCommitBatchSize}",
			"txtBatchNumRows": "1000",
            "commitSequenceKeyName": "#syncCommitSequence",
            "keyCols": "${keyTokens}",
            "otherCols": "${otherTokens}"
          }
        }
      },
      "sequence": [
        "getFromSource",
        "getFromCopy",
        "diff",
        "sync"
      ]
    }
  },
  "sequence": [
    "syncTransformName"
  ]
}`
