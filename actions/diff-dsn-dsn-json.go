package actions

var jsonDiffDsnDsn = `{
  "schemaVersion": 3,
  "description": "sync batch from Oracle to Oracle",
  "connections": {
    "source": {
      "type": "${sourceType}",
      "logicalName": "${sourceEnv}",
      "data": {
        "dsn": "${sourceDsn}"
      }
    },
    "target": {
      "type": "${targetType}",
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
    "diffTransform": {
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
            "sqlText": "select ${columnListCsv} from ${targetTable} order by ${SQLPrimaryKeyFieldsCsv}"
          }
        },
        "diff": {
          "type": "MergeDiff",
          "data": {
            "readOldDataFromStep": "getFromTarget",
            "readNewDataFromStep": "getFromSource",
            "joinKeys": "${keyTokens}",
            "compareKeys": "${otherTokens}",
            "flagFieldName": "#diffStatus",
            "outputIdenticalRows": "false"
          }
        },
		"fieldMapper": {
          "type": "FieldMapper",
          "data": {
            "readDataFromStep": "diff"
          },
          "steps": [
            {
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#diffStatus",
                "regexpMatch": "${new}",
                "regexpReplace": "NEW",
                "resultField": "#diffStatus",
				"propagateInput": "true"
              }
            },
			{
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#diffStatus",
                "regexpMatch": "^${changed}$",
                "regexpReplace": "CHANGED",
                "resultField": "#diffStatus",
				"propagateInput": "true"
              }
            },
			{
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#diffStatus",
                "regexpMatch": "^${deleted}$",
                "regexpReplace": "DELETED",
                "resultField": "#diffStatus",
				"propagateInput": "true"
              }
            },
			{
              "type": "RegexpReplace",
              "data": {
                "fieldName": "#diffStatus",
                "regexpMatch": "^${identical}$",
                "regexpReplace": "IDENTICAL",
                "resultField": "#diffStatus",
				"propagateInput": "true"
              }
            }
          ]
        },
        "stdout": {
          "type": "StdOutPassThrough",
          "data": {
			"readDataFromStep": "fieldMapper",
			"outputFieldsCsv": ${outputFieldsCsv},
			"abortAfterNumRecords": "${abortAfterNumRecords}"
		  }
        }
      },
      "sequence": [
        "getFromSource",
        "getFromTarget",
        "diff",
		"fieldMapper",
        "stdout"
      ]
    }
  },
  "sequence": [
    "diffTransform"
  ]
}`
