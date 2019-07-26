package heptane

import (
	"fmt"
	"testing"

	cm "github.com/heptanes/heptane/cache/mock"
	r "github.com/heptanes/heptane/row"
	rm "github.com/heptanes/heptane/row/mock"
)

func TestingTable1() r.Table {
	return r.Table{
		Name:         "table1",
		PartitionKey: []r.FieldName{"foo"},
		PrimaryKey:   []r.FieldName{"foo", "bar"},
		Values:       []r.FieldName{"baz"},
		Types:        r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string"}, PrimaryKeyCachePrefix: []string{"table1_pk", "0"},
	}
}

func TestHeptane_Register_InvalidTable(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Name = ""
	if err := h.Register(b, nil, nil); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Empty TableName in Table" {
		t.Error(s)
	}
}

func TestHeptane_Register_InvalidRowProvider(t *testing.T) {
	h := New()
	b := TestingTable1()
	if err := h.Register(b, nil, nil); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != "Null RowProvider for Table table1" {
		t.Error(s)
	}
}

func TestHeptane_Register_OK(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
}

func TestHeptane_TableNames(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprint(h.TableNames()); s != "[table1]" {
		t.Error(s)
	}
}

func TestHeptane_Table(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", h.Table(b.Name)); s != `heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}` {
		t.Error(s)
	}
}

func TestHeptane_RowProvider(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if rp := h.RowProvider(b.Name); rp != rm {
		t.Error(rp)
	}
}

func TestHeptane_CacheProvider(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	if cp := h.CacheProvider(b.Name); cp != cm {
		t.Error(cp)
	}
}

func TestHeptane_Unregister(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	h.Unregister(b.Name)
	if s := fmt.Sprint(h.TableNames()); s != "[]" {
		t.Error(s)
	}
}

func TestHeptane_UnsupportedAccessTypeError(t *testing.T) {
	h := New()
	if err := h.Access(Retrieve{}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported Access Type: heptane.Retrieve{TableName:"", FieldValues:heptane.FieldValuesByName(nil), RetrievedValues:[]heptane.FieldValuesByName(nil)}` {
		t.Error(s)

	}
}
