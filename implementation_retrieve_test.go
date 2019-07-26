package heptane

import (
	"errors"
	"fmt"
	"testing"

	c "github.com/heptanes/heptane/cache"
	cm "github.com/heptanes/heptane/cache/mock"
	r "github.com/heptanes/heptane/row"
	rm "github.com/heptanes/heptane/row/mock"
)

func TestHeptane_Retrieve_UnknownTable(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(&Retrieve{"unknown", nil, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unregistered Table unknown` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_MissingPartitionKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Missing FieldValue for Field table1.foo: map[]` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_InvalidPartitionKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": 1}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 1` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_MissingPrimaryKey_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"}}, errors.New("problem1"))
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowRetrieve{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_MissingPrimaryKey_OK_Single(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_MissingPrimaryKey_OK_Multiple(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
			r.FieldValuesByName{"foo": "1", "bar": "4"},
		}}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}, heptane.FieldValuesByName{"bar":"4", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_InvalidPrimaryKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": 2}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 2` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithPrimaryKey_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, errors.New("problem1"))
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowRetrieve{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithPrimaryKey_OK(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	if err := h.Register(b, rm, nil); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_MissingPrimaryKey_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"}}, errors.New("problem1"))
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowRetrieve{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)} Error: problem1` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_MissingPrimaryKey_CacheAccessError_Single(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, errors.New("problem"))
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}
	if err := h.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.CacheSet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue{0x73, 0x33}} Error: problem` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_MissingPrimaryKey_CacheAccessError_Multiple(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
			r.FieldValuesByName{"foo": "1", "bar": "4"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, errors.New("problem1"))
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s4", Value: c.CacheValue("")}, errors.New("problem2"))
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}
	if err := h.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Multiple Errors: [heptane.CacheSet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue{0x73, 0x33}} Error: problem1 heptane.CacheSet{Key:"table1_pk#0#s1#s4", Value:heptane.CacheValue{}} Error: problem2]` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_MissingPrimaryKey_OK_Single(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_MissingPrimaryKey_OK_Multiple(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
			r.FieldValuesByName{"foo": "1", "bar": "4"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s4", Value: c.CacheValue("")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}, heptane.FieldValuesByName{"bar":"4", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_InvalidPrimaryKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": 2}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 2` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2"}, errors.New("problem"))
	if err := h.Access(&Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.CacheGet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue(nil)} Error: problem` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheHit_WithValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheHit_WithoutValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheHit_TruncatedValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_RowAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"}}, errors.New("problem"))
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.RowRetrieve{Table:heptane.Table{Name:"table1", PartitionKey:[]heptane.FieldName{"foo"}, PrimaryKey:[]heptane.FieldName{"foo", "bar"}, Values:[]heptane.FieldName{"baz"}, Types:heptane.FieldTypesByName{"bar":"string", "baz":"string", "foo":"string"}, PrimaryKeyCachePrefix:[]string{"table1_pk", "0"}}, FieldValues:heptane.FieldValuesByName{"bar":"2", "foo":"1"}, RetrievedValues:[]heptane.FieldValuesByName(nil)} Error: problem` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_RowInvalidResponse_PrimaryKey(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": 1, "bar": "2", "baz": "3"},
		}}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 1` {
		t.Error(s)

	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_RowInvalidResponse_Value(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": 3},
		}}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `Unsupported FieldValue for FieldType string: 3` {
		t.Error(s)

	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_CacheAccessError(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, errors.New("problem"))
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err == nil {
		t.Error(err)
	} else if s := err.Error(); s != `heptane.CacheSet{Key:"table1_pk#0#s1#s2", Value:heptane.CacheValue{0x73, 0x33}} Error: problem` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_OK_WithValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_OK_WithoutValue(t *testing.T) {
	h := New()
	b := TestingTable1()
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_OK_MultipleValues_1(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("#")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_OK_MultipleValues_2(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "baz": "3"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("s3#")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "baz":"3", "foo":"1"}}` {
		t.Error(s)
	}
}

func TestHeptane_Retrieve_WithCache_WithPrimaryKey_CacheMiss_OK_MultipleValues_3(t *testing.T) {
	h := New()
	b := TestingTable1()
	b.Values = []r.FieldName{"baz", "qux"}
	b.Types = r.FieldTypesByName{"foo": "string", "bar": "string", "baz": "string", "qux": "string"}
	rm := &rm.Row{}
	cm := &cm.Cache{}
	if err := h.Register(b, rm, cm); err != nil {
		t.Error(err)
	}
	cm.Mock(c.CacheGet{Key: "table1_pk#0#s1#s2", Value: nil}, nil)
	rm.Mock(r.RowRetrieve{Table: b, FieldValues: r.FieldValuesByName{"foo": "1", "bar": "2"},
		RetrievedValues: []r.FieldValuesByName{
			r.FieldValuesByName{"foo": "1", "bar": "2", "qux": "4"},
		}}, nil)
	cm.Mock(c.CacheSet{Key: "table1_pk#0#s1#s2", Value: c.CacheValue("#s4")}, nil)
	a := &Retrieve{b.Name, r.FieldValuesByName{"foo": "1", "bar": "2"}, nil}
	if err := h.Access(a); err != nil {
		t.Error(err)
	}
	if s := fmt.Sprintf("%#v", a.RetrievedValues); s != `[]heptane.FieldValuesByName{heptane.FieldValuesByName{"bar":"2", "foo":"1", "qux":"4"}}` {
		t.Error(s)
	}
}
