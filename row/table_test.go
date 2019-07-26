package heptane

import (
	"encoding/json"
	"testing"
)

func TestingTable() Table {
	return Table{
		Name:                  "table",
		PartitionKey:          []FieldName{"foo"},
		PrimaryKey:            []FieldName{"foo", "bar"},
		Values:                []FieldName{"baz"},
		Types:                 FieldTypesByName{"foo": "string", "bar": "string", "baz": "bool"},
		PrimaryKeyCachePrefix: []string{"table_pk", "0"},
	}
}

func TestTable_Json(t *testing.T) {
	b := TestingTable()
	if q, err := json.Marshal(b); err != nil {
		t.Fatal(err)
	} else if s := string(q); s != `{"name":"table","partitionKey":["foo"],"primaryKey":["foo","bar"],"values":["baz"],"types":{"bar":"string","baz":"bool","foo":"string"},"primaryKeyCachePrefix":["table_pk","0"]}` {
		t.Error(s)
	}
}

func TestTable_Validate_Name(t *testing.T) {
	b := TestingTable()
	b.Name = ""
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Empty TableName in Table" {
		t.Error(s)
	}
}

func TestTable_Validate_PartitionKey_Empty(t *testing.T) {
	b := TestingTable()
	b.PartitionKey = []FieldName{}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Missing PartitionKey" {
		t.Error(s)
	}
}

func TestTable_Validate_PartitionKey_EmptyFieldName(t *testing.T) {
	b := TestingTable()
	b.PartitionKey = []FieldName{""}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Empty FieldName in PartitionKey" {
		t.Error(s)
	}
}

func TestTable_Validate_PartitionKey_Repeated(t *testing.T) {
	b := TestingTable()
	b.PartitionKey = []FieldName{"foo", "foo"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Repeated FieldName in PartitionKey: foo" {
		t.Error(s)
	}
}

func TestTable_Validate_PrimaryKey_Empty(t *testing.T) {
	b := TestingTable()
	b.PrimaryKey = []FieldName{}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Missing PrimaryKey" {
		t.Error(s)
	}
}

func TestTable_Validate_PrimaryKey_EmptyFieldName(t *testing.T) {
	b := TestingTable()
	b.PrimaryKey = []FieldName{""}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Empty FieldName in PrimaryKey" {
		t.Error(s)
	}
}

func TestTable_Validate_PrimaryKey_Mismatched(t *testing.T) {
	b := TestingTable()
	b.PrimaryKey = []FieldName{"bar"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Mismatched PrimaryKey: bar" {
		t.Error(s)
	}
}

func TestTable_Validate_PrimaryKey_Repeated(t *testing.T) {
	b := TestingTable()
	b.PrimaryKey = []FieldName{"foo", "foo"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Repeated FieldName in PrimaryKey: foo" {
		t.Error(s)
	}
}

func TestTable_Validate_Values_EmptyFieldName(t *testing.T) {
	b := TestingTable()
	b.Values = []FieldName{""}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Empty FieldName in Values" {
		t.Error(s)
	}
}

func TestTable_Validate_Values_Repeated_PartitionKey(t *testing.T) {
	b := TestingTable()
	b.Values = []FieldName{"foo"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Repeated FieldName in Values: foo" {
		t.Error(s)
	}
}

func TestTable_Validate_Values_Repeated_PrimaryKey(t *testing.T) {
	b := TestingTable()
	b.Values = []FieldName{"bar"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Repeated FieldName in Values: bar" {
		t.Error(s)
	}
}

func TestTable_Validate_Values_Repeated_Values(t *testing.T) {
	b := TestingTable()
	b.Values = []FieldName{"baz", "baz"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Repeated FieldName in Values: baz" {
		t.Error(s)
	}
}

func TestTable_Validate_Types_Missing_PartitionKey(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Missing FieldType for FieldName: foo" {
		t.Error(s)
	}
}

func TestTable_Validate_Types_Missing_PrimaryKey(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "string"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Missing FieldType for FieldName: bar" {
		t.Error(s)
	}
}

func TestTable_Validate_Types_Missing_Values(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "string", "bar": "string"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Missing FieldType for FieldName: baz" {
		t.Error(s)
	}
}

func TestTable_Validate_Types_Invalid_PartitionKey(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "unknown"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Invalid FieldType for FieldName foo: unknown" {
		t.Error(s)
	}
}

func TestTable_Validate_Types_Invalid_PrimaryKey(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "string", "bar": "unknown"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Invalid FieldType for FieldName bar: unknown" {
		t.Error(s)
	}
}

func TestTable_Validate_Types_Invalid_Values(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "string", "bar": "string", "baz": "unknown"}
	if err := b.Validate(); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Table table: Invalid FieldType for FieldName baz: unknown" {
		t.Error(s)
	}
}

func TestTable_Validate_OK(t *testing.T) {
	b := TestingTable()
	if err := b.Validate(); err != nil {
		t.Error(err)
	}
}

func TestTable_Validate_ValidType_String(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "string", "bar": "string", "baz": "string"}
	if err := b.Validate(); err != nil {
		t.Error(err)
	}
}

func TestTable_Validate_ValidType_Bool(t *testing.T) {
	b := TestingTable()
	b.Types = FieldTypesByName{"foo": "bool", "bar": "bool", "baz": "bool"}
	if err := b.Validate(); err != nil {
		t.Error(err)
	}
}
