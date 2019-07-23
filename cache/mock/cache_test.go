package heptane

import (
	"errors"
	"testing"

	"github.com/heptanes/heptane"
)

func TestAccess_Unsupported_Get(t *testing.T) {
	p := Cache{}
	a := heptane.CacheGet{Key: "foo"}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Unsupported heptane.CacheAccess Type: heptane.CacheGet` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_Get(t *testing.T) {
	p := Cache{}
	a := &heptane.CacheGet{Key: "foo"}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: &heptane.CacheGet{Key:"foo", Value:heptane.CacheValue(nil)}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_Get(t *testing.T) {
	p := Cache{}
	p.Mock(heptane.CacheGet{Key: "bogus", Value: []byte("bogus")}, errors.New("bogus"))
	p.Mock(heptane.CacheGet{Key: "foo", Value: []byte("bar")}, errors.New("baz"))
	a := &heptane.CacheGet{Key: "foo"}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `baz` {
		t.Error(s)
	}
	if s := string(a.Value); s != `bar` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_Get(t *testing.T) {
	p := Cache{}
	p.Mock(&heptane.CacheGet{Key: "bogus", Value: []byte("bogus")}, errors.New("bogus"))
	p.Mock(&heptane.CacheGet{Key: "foo", Value: []byte("bar")}, errors.New("baz"))
	a := &heptane.CacheGet{Key: "foo"}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `baz` {
		t.Error(s)
	}
	if s := string(a.Value); s != `bar` {
		t.Error(s)
	}
}

func TestAccess_Unmocked_NormalSet(t *testing.T) {
	p := Cache{}
	a := heptane.CacheSet{Key: "foo", Value: []byte("bar")}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `Not Mocked: heptane.CacheSet{Key:"foo", Value:heptane.CacheValue{0x62, 0x61, 0x72}}` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_NormalSet(t *testing.T) {
	p := Cache{}
	p.Mock(heptane.CacheSet{Key: "bogus", Value: []byte("baz")}, errors.New("bogus"))
	p.Mock(heptane.CacheSet{Key: "foo", Value: []byte("bogus")}, errors.New("bogus"))
	p.Mock(heptane.CacheSet{Key: "foo", Value: []byte("bar")}, errors.New("baz"))
	a := heptane.CacheSet{Key: "foo", Value: []byte("bar")}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `baz` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_NormalSet(t *testing.T) {
	p := Cache{}
	p.Mock(&heptane.CacheSet{Key: "bogus", Value: []byte("baz")}, errors.New("bogus"))
	p.Mock(&heptane.CacheSet{Key: "foo", Value: []byte("bogus")}, errors.New("bogus"))
	p.Mock(&heptane.CacheSet{Key: "foo", Value: []byte("bar")}, errors.New("baz"))
	a := heptane.CacheSet{Key: "foo", Value: []byte("bar")}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `baz` {
		t.Error(s)
	}
}

func TestAccess_NormalMocked_RefSet(t *testing.T) {
	p := Cache{}
	p.Mock(heptane.CacheSet{Key: "bogus", Value: []byte("baz")}, errors.New("bogus"))
	p.Mock(heptane.CacheSet{Key: "foo", Value: []byte("bogus")}, errors.New("bogus"))
	p.Mock(heptane.CacheSet{Key: "foo", Value: []byte("bar")}, errors.New("baz"))
	a := &heptane.CacheSet{Key: "foo", Value: []byte("bar")}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `baz` {
		t.Error(s)
	}
}

func TestAccess_RefMocked_RefSet(t *testing.T) {
	p := Cache{}
	p.Mock(&heptane.CacheSet{Key: "bogus", Value: []byte("baz")}, errors.New("bogus"))
	p.Mock(&heptane.CacheSet{Key: "foo", Value: []byte("bogus")}, errors.New("bogus"))
	p.Mock(&heptane.CacheSet{Key: "foo", Value: []byte("bar")}, errors.New("baz"))
	a := &heptane.CacheSet{Key: "foo", Value: []byte("bar")}
	err := p.Access(a)
	if err == nil {
		t.Fatal(err)
	}
	if s := err.Error(); s != `baz` {
		t.Error(s)
	}
}

func TestAccessSlice(t *testing.T) {
	p := Cache{}
	a := heptane.CacheSet{Key: "foo", Value: []byte("bar")}
	errs := p.AccessSlice([]heptane.CacheAccess{a})
	if errs == nil {
		t.Error(errs)
	}
	if l := len(errs); l != 1 {
		t.Fatal(l)
	}
	err := errs[0]
	if s := err.Error(); s != `Not Mocked: heptane.CacheSet{Key:"foo", Value:heptane.CacheValue{0x62, 0x61, 0x72}}` {
		t.Error(s)
	}
}
