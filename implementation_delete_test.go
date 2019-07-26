package heptane

import (
	"errors"
	"testing"

	c "github.com/heptanes/heptane/cache"
	cm "github.com/heptanes/heptane/cache/mock"
	r "github.com/heptanes/heptane/row"
	rm "github.com/heptanes/heptane/row/mock"
)

func TestHeptane_Delete_UnknownTable(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Delete{"unknown", nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unregistered Table unknown` {
		t.Error(s)
	}
}

func TestHeptane_Delete_MissingPrimaryKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Missing FieldValue for Field table1.foo: map[]` {
		t.Error(s)
	}
}

func TestHeptane_Delete_InvalidPrimaryKeyType(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": "1", "bar": 2}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 2` {
		t.Error(s)
	}
}

func TestHeptane_Delete_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, errors.New("problem1"))
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowDelete{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "foo":"1"}} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Delete_OK(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Delete_OK_BySlice_ByRef(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	a := &Delete{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}
	if errs := h.AccessSlice([]Access{a}); errs == nil {
		t.Error(errs)
	} else if l := len(errs); l != 1 {
		t.Error(l)
	} else if err := errs[0]; err != nil {
		t.Error(err)
	}
}

func TestHeptane_Delete_WithCache_CacheAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: nil}, errors.New("problem1"))
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.CacheSet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue(nil)} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Delete_WithCache_OK(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Delete_Bool_InvalidValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Types = r.FieldTypesByName{"foo": "bool", "bar": "bool", "baz": "bool"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": "invalid", "bar": true}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType bool: invalid` {
		t.Error(s)
	}
}

func TestHeptane_Delete_Bool_OK(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Types = r.FieldTypesByName{"foo": "bool", "bar": "bool", "baz": "bool"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowDelete{Table: b, FieldValues: r.FieldValuesByName{"foo": false, "bar": true}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#f#t", Value: nil}, nil)
	if err := h.Access(Delete{b.Name, r.FieldValuesByName{"foo": false, "bar": true}}); err != nil {
		t.Error(err)
	}
}
