package heptane

import (
	"encoding/json"
	"testing"
)

func TestTableCanBeSerializedAsJson(t *testing.T) {
	r := Table{
		Name:                  "foo",
		PartitionKey:          []FieldName{"bar"},
		PrimaryKey:            []FieldName{"bar"},
		Values:                []FieldName{"baz"},
		Types:                 FieldTypesByName{"bar": "string", "baz": "int"},
		PrimaryKeyCachePrefix: []CacheKey{"foo_pk", "0"},
	}
	if b, err := json.Marshal(r); err != nil {
		t.Fatal(err)
	} else if s := string(b); s != `{"name":"foo","partitionKey":["bar"],"primaryKey":["bar"],"values":["baz"],"types":{"bar":"string","baz":"int"},"primaryKeyCachePrefix":["foo_pk","0"]}` {
		t.Error(s)
	}
}
