package actions

var jsonSyncCqnSnowflake = `{
  "schemaVersion": 3,
  "description": "sync events from Oracle to Snowflake",
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
  "type": "sequential",
  "transformGroups": {
    "cqn": {
      "type": "sequential",
      "steps": {
        "cqnSqlInput": {
          "type": "OracleContinuousQueryNotificationToSnowflake",
          "data": {
            "sourceDatabaseConnectionName": "source",
            "targetDatabaseConnectionName": "target",
            "sourceSqlQuery": "select ${columnListCsv}, rowid as ORIGIN_ROWID from ${sourceTable} order by ${SQLPrimaryKeyFieldsCsv}",
            "targetSqlQuery": "select ${columnListCsv},\"${targetTableOriginRowIdFieldName}\" from ${targetTable} order by ${SQLPrimaryKeyFieldsCsv}",
            "sourceRowIdColumnName": "origin_rowid",
            "targetRowIdColumnName": "${targetTableOriginRowIdFieldName}",
            "targetSchemaName": "${syncTargetSchema}",
            "targetTableName": "${syncTargetTable}",
            "primaryKeyColumns": "${keyTokens}",
            "otherColumns": "${otherTokens},ORIGIN_ROWID:${targetTableOriginRowIdFieldName}",
            "snowflakeStageName": "${snowflakeStageName}",
            "bucketRegion": "${bucketRegion}",
            "bucketPrefix": "${bucketPrefix}",
            "bucketName": "${bucketName}",
			"csvFileNamePrefix": "${csvFileNamePrefix}",
			"csvHeaderFieldsCSV": "${csvHeaderFieldsCSV},\"${targetTableOriginRowIdFieldName}\"",
            "csvMaxFileBytes": "${csvMaxFileBytes}",
            "csvMaxFileRows": "${csvMaxFileRows}"
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
