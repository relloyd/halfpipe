package shared

func FixSqlStatementGeneratorConfig(cfg *SqlStatementGeneratorConfig) {
	if cfg.OutputTable == "" {
		cfg.Log.Fatal("Error, missing output table name.")
	}
	if cfg.OutputSchema == "" {
		cfg.SchemaSeparator = ""
		cfg.Log.Debug("No output schema supplied; setting a blank separator.")
	} else {
		cfg.SchemaSeparator = "."
	}
}
