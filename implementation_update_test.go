package heptane

import (
	"errors"
	"testing"

	c "github.com/heptanes/heptane/cache"
	cm "github.com/heptanes/heptane/cache/mock"
	r "github.com/heptanes/heptane/row"
	rm "github.com/heptanes/heptane/row/mock"
)

func TestHeptane_Update_UnknownTable(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Update{"unknown", nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unregistered Table unknown` {
		t.Error(s)
	}
}

func TestHeptane_Update_MissingPrimaryKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Update{b.Name, r.FieldValuesByName{}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Missing FieldValue for Field table1.foo: map[]` {
		t.Error(s)
	}
}

func TestHeptane_Update_InvalidPrimaryKeyType(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": 2}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 2` {
		t.Error(s)
	}
}

func TestHeptane_Update_InvalidValueType(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": 3}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 3` {
		t.Error(s)
	}
}

func TestHeptane_Update_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}, errors.New("problem1"))
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowUpdate{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Update_OK_MissingValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_OK_NullValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_OK_NotNullValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_OK_BySlice_ByRef(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}, nil)
	a := &Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}
	if errs := h.AccessSlice([]Access{a}); errs == nil {
		t.Error(errs)
	} else if l := len(errs); l != 1 {
		t.Error(l)
	} else if err := errs[0]; err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_OK_Multiplevalues(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3", "qux": "4"}}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3", "qux": "4"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_WithCache_Full_CacheAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, errors.New("problem1"))
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.CacheSet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue{0x73, 0x33}} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Update_WithCache_Full_OK_NullValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("")}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_WithCache_Full_OK_NotNullValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_WithCache_Partial_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, errors.New("problem"))
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowRetrieve{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)} Error: problem` {
		t.Error(s)
	}
}

func TestHeptane_Update_WithCache_Partial_RowInvalidResponse(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": 3},
		}}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 3` {
		t.Error(s)
	}
}

func TestHeptane_Update_WithCache_Partial_CacheAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, errors.New("problem"))
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.CacheSet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue{0x73, 0x33}} Error: problem` {
		t.Error(s)
	}
}

func TestHeptane_Update_WithCache_Partial_OK_MissingValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_WithCache_Partial_OK_NullValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "qux": "4"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("#s4")}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": nil}}); err != nil {
		t.Error(err)
	}
}

func TestHeptane_Update_WithCache_Partial_OK_NotNullValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowUpdate{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	if err := h.Access(Update{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"}}); err != nil {
		t.Error(err)
	}
}
