package actions

var jsonSyncCqnOracle = `{
  "schemaVersion": 3,
  "description": "sync events from Oracle to Oracle",
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
  "type": "sequential",
  "transformGroups": {
    "cqn": {
      "type": "sequential",
      "steps": {
        "cqnSqlInput": {
          "type": "OracleContinuousQueryNotificationToRdbms",
          "data": {
            "sourceDatabaseConnectionName": "source",
            "targetDatabaseConnectionName": "target",
            "sourceSqlQuery": "select ${columnListCsv}, rowid as origin_rowid from ${sourceTable} order by ${SQLPrimaryKeyFieldsCsv}",
            "targetSqlQuery": "select ${columnListCsv}, origin_rowid from ${targetTable} order by ${SQLPrimaryKeyFieldsCsv}",
            "sourceRowIdColumnName": "origin_rowid",
            "targetRowIdColumnName": "${targetTableOriginRowIdFieldName}",
            "targetSchemaName": "${syncTargetSchema}",
            "targetTableName": "${syncTargetTable}",
            "primaryKeyColumns": "${keyTokens}",
            "otherColumns": "${otherTokens},ORIGIN_ROWID:${targetTableOriginRowIdFieldName}",
            "batchSize": "${commitBatchSize}"
          }
        }
      },
      "sequence": [
        "cqnSqlInput"
      ]
    }
  },
  "sequence": [
    "cqn"
  ]
}
`
