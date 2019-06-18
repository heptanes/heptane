package heptane

import (
	"encoding/json"
	"testing"
)

func TestTableCanBeSerializedAsJson(t *testing.T) {
	r := Table{
		Name:         "foo",
		Fields:       map[FieldName]FieldType{"bar": "string", "baz": "int"},
		PrimaryKey:   []FieldName{"bar"},
		PartitionKey: []FieldName{"bar"},
		PrimaryKeyCache: TablePrimaryKeyCache{
			Enabled:      true,
			CachePrefix:  "foo_pk",
			CacheVersion: 0,
		},
	}
	if b, err := json.Marshal(r); err != nil {
		t.Fatal(err)
	} else if s := string(b); s != `{"name":"foo","fields":{"bar":"string","baz":"int"},"primaryKey":["bar"],"partitionKey":["bar"],"primaryKeyCache":{"enabled":true,"cachePrefix":"foo_pk","cacheVersion":0}}` {
		t.Error(s)
	}
}
