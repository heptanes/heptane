package heptane

import (
	"errors"
	"fmt"
	"testing"

	r "github.com/heptanes/heptane/row"
)

var (
	table = r.Table{
		Name:                  "table",
		PartitionKey:          []r.FieldName{"foo"},
		PrimaryKey:            []r.FieldName{"foo", "bar"},
		Values:                []r.FieldName{"baz"},
		Types:                 r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string"},
		PrimaryKeyCachePrefix: []string{"table_pk", "0"},
	}
	bogusTable = r.Table{
		Name:                  "bogus",
		PartitionKey:          table.PartitionKey,
		PrimaryKey:            table.PrimaryKey,
		Values:                table.Values,
		Types:                 table.Types,
		PrimaryKeyCachePrefix: []string{"bogus_pk", "0"},
	}
	primaryFieldsValues  = r.FieldValuesByName{"foo": "1", "bar": "2"}
	allFieldsValues      = r.FieldValuesByName{"foo": "1", "bar": "2", "baz": 3}
	allFieldsValuesSlice = []r.FieldValuesByName{allFieldsValues}
)

func TestAccess_Unsupported_Retrieve(t *testing.T) {
	p := Row{}
	a := r.RowRetrieve{Table: table, FieldValues: allFieldsValues}
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
	a := r.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowCreate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_RefCreate(t *testing.T) {
	p := Row{}
	a := &r.RowCreate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowCreate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalCreate(t *testing.T) {
	p := Row{}
	p.Mock(r.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowCreate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := r.RowCreate{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(&r.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowCreate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := r.RowCreate{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(r.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowCreate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &r.RowCreate{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(&r.RowCreate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowCreate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowCreate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &r.RowCreate{Table: table, FieldValues: allFieldsValues}
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
	a := &r.RowRetrieve{Table: table, FieldValues: primaryFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: &heptane.RowRetrieve{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_Retrieve(t *testing.T) {
	p := Row{}
	p.Mock(r.RowRetrieve{Table: bogusTable, FieldValues: primaryFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowRetrieve{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowRetrieve{Table: table, FieldValues: primaryFieldsValues,
		RetrievedValues: allFieldsValuesSlice}, errors.New("err"))
	a := &r.RowRetrieve{Table: table, FieldValues: primaryFieldsValues}
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
	p.Mock(&r.RowRetrieve{Table: bogusTable, FieldValues: primaryFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowRetrieve{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowRetrieve{Table: table, FieldValues: primaryFieldsValues,
		RetrievedValues: allFieldsValuesSlice}, errors.New("err"))
	a := &r.RowRetrieve{Table: table, FieldValues: primaryFieldsValues}
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
	a := &r.RowUpdate{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowUpdate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalUpdate(t *testing.T) {
	p := Row{}
	p.Mock(r.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowUpdate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := r.RowUpdate{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(&r.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowUpdate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := r.RowUpdate{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(r.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowUpdate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &r.RowUpdate{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(&r.RowUpdate{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowUpdate{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowUpdate{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &r.RowUpdate{Table: table, FieldValues: allFieldsValues}
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
	a := &r.RowDelete{Table: table, FieldValues: allFieldsValues}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.RowDelete{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalDelete(t *testing.T) {
	p := Row{}
	p.Mock(r.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowDelete{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := r.RowDelete{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(&r.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowDelete{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := r.RowDelete{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(r.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(r.RowDelete{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(r.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &r.RowDelete{Table: table, FieldValues: allFieldsValues}
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
	p.Mock(&r.RowDelete{Table: bogusTable, FieldValues: allFieldsValues}, errors.New("bogus"))
	p.Mock(&r.RowDelete{Table: table, FieldValues: r.FieldValuesByName{}}, errors.New("bogus"))
	p.Mock(&r.RowDelete{Table: table, FieldValues: allFieldsValues}, errors.New("err"))
	a := &r.RowDelete{Table: table, FieldValues: allFieldsValues}
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
	a := r.RowCreate{Table: table, FieldValues: allFieldsValues}
	errs := p.AccessSlice([]r.RowAccess{a})
	if errs == nil {
		t.Error(errs)
	}
	if l := len(errs); l != 1 {
		t.Fatal(l)
	}
	err := errs[0]
	if s := err.Error(); s != `Not Mocked: heptane.RowCreate{Table:heptane.Table{Name:"table", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":3, "foo":"1"}}` {
		t.Error(s)
	}
}
