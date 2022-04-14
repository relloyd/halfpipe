package tabledefinition

import (
	"github.com/sirupsen/logrus"
	"testing"
)

func TestTableDefinitionMapper(t *testing.T) {
	log := logrus.New()
	level, _ := logrus.ParseLevel("debug")
	log.SetLevel(level)
	log.Info("Testing TestTableDefinitionMapper()...")
	mapper := NewOracleToSnowflakeDataTypeMapper()
	dtMapping := OracleToSnowflakeDataTypeMapping
	i1 := dtMapping[0].SourceDataType
	o1 := mapper.Map(i1)
	log.Debug("Mapped ", i1, " to ", o1)
	if o1 != dtMapping[0].TargetDataType {
		t.Fatal("Input not converted to the correct output.")
	}
	return
}

func TestSnowflakeToSnowflakeDataTypeMapping(t *testing.T) {
	for i, v := range SnowflakeToSnowflakeDataTypeMapping {
		if v.SourceDataType != v.TargetDataType {
			t.Fatalf("expected src and tgt data types to match in SnowflakeToSnowflakeDataTypeMapping entry %v: src = %v; tgt = %v", i, v.SourceDataType, v.TargetDataType)
		}
	}
}
