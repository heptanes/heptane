package heptane

import (
	"errors"
	"fmt"
	"testing"

	"github.com/heptanes/heptane"
)

var (
	table = heptane.Table{
		Name:                  "table",
		PartitionKey:          []heptane.FieldName{"foo"},
		PrimaryKey:            []heptane.FieldName{"foo", "bar"},
		Values:                []heptane.FieldName{"baz"},
		Types:                 heptane.FieldTypesByName{"foo": "string", "bar": "string", "baz": "int"},
		PrimaryKeyCachePrefix: []heptane.CacheKey{"table_pk", "0"},
	}
	bogusTable = heptane.Table{
		Name:                  "bogus",
		PartitionKey:          table.PartitionKey,
		PrimaryKey:            table.PrimaryKey,
		Values:                table.Values,
		Types:                 table.Types,
		PrimaryKeyCachePrefix: []heptane.CacheKey{"bogus_pk", "0"},
	}
	primaryFieldsValues  = heptane.FieldValuesByName{"foo": "1", "bar": "2"}
	allFieldsValues      = heptane.FieldValuesByName{"foo": "1", "bar": "2", "baz": 3}
	allFieldsValuesSlice = []heptane.FieldValuesByName{allFieldsValues}
)

func TestAccess_Unsupported_Retrieve(t *testing.T) {
	p := Row{}
	a := heptane.RowRetrieve{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Unsupported heptane.RowAccess Type: heptane.RowRetrieve` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_NormalCreate(t *testing.T) {
	p := Row{}
	a := heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowCreate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"int", "foo":"string"}, PrimaryKeyCachePrefix:[]heptane.CacheKey{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_RefCreate(t *testing.T) {
	p := Row{}
	a := &heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowCreate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"int", "foo":"string"}, PrimaryKeyCachePrefix:[]heptane.CacheKey{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalCreate(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowCreate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_NormalCreate(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowCreate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_RefCreate(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowCreate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_RefCreate(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowCreate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_Retrieve(t *testing.T) {
	p := Row{}
	a := &heptane.RowRetrieve{Table: table, FieldValues: primaryFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: &heptane.RowRetrieve{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"int", "foo":"string"}, PrimaryKeyCachePrefix:[]heptane.CacheKey{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_Retrieve(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowRetrieve{Table: bogusTable, FieldValues: primaryFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowRetrieve{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowRetrieve{Table: table, FieldValues: primaryFieldsValues,
		RetrievedValues: allFieldsValuesSlice}, errors.New("err"))
	a := &heptane.RowRetrieve{Table: table, FieldValues: primaryFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
	if s := fmt.Sprint(a.RetrievedValues); s != fmt.Sprint(allFieldsValuesSlice) {
		t.Error(s)
	}
}

func TestAccess_RefMocked_Retrieve(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowRetrieve{Table: bogusTable, FieldValues: primaryFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowRetrieve{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowRetrieve{Table: table, FieldValues: primaryFieldsValues,
		RetrievedValues: allFieldsValuesSlice}, errors.New("err"))
	a := &heptane.RowRetrieve{Table: table, FieldValues: primaryFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
	if s := fmt.Sprint(a.RetrievedValues); s != fmt.Sprint(allFieldsValuesSlice) {
		t.Error(s)
	}
}

func TestAccess_Unmocked_RefUpdate(t *testing.T) {
	p := Row{}
	a := &heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowUpdate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"int", "foo":"string"}, PrimaryKeyCachePrefix:[]heptane.CacheKey{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalUpdate(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowUpdate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_NormalUpdate(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowUpdate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_RefUpdate(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowUpdate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_RefUpdate(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowUpdate{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &heptane.RowUpdate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_RefDelete(t *testing.T) {
	p := Row{}
	a := &heptane.RowDelete{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowDelete{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"int", "foo":"string"}, PrimaryKeyCachePrefix:[]heptane.CacheKey{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalDelete(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowDelete{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := heptane.RowDelete{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_NormalDelete(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowDelete{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := heptane.RowDelete{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_RefDelete(t *testing.T) {
	p := Row{}
	p.Mock(heptane.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(heptane.RowDelete{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(heptane.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &heptane.RowDelete{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_RefDelete(t *testing.T) {
	p := Row{}
	p.Mock(&heptane.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&heptane.RowDelete{Table: table, FieldValues: heptane.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&heptane.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &heptane.RowDelete{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `err` {
		t.Error(s)
	}
}

func TestAccessSlice(t *testing.T) {
	p := Row{}
	a := heptane.RowCreate{Table: table, FieldValues: allFieldsValues}
	errs := p.AccessSlice([]heptane.RowAccess{a})
	if errs == nil {
		t.Error(errs)
	}
	if l := len(errs); l != 1 {
		t.Fatal(l)
	}
	err := errs[0]
	if s := err.Error(); s != `Not Mocked: heptane.RowCreate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"int", "foo":"string"}, PrimaryKeyCachePrefix:[]heptane.CacheKey{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}
