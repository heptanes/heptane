package heptane

import (
	"bytes"
	"fmt"

	"github.com/heptanes/heptane"
)

type access struct {
	a heptane.CacheAccess
	e error
}

// Cache implements CacheProvider.
type Cache struct {
	s []access
}

// Mock ensures the following calls to Access() with the given input fields of
// CacheAccess will return the given output fields of CacheAccess and the given
// error.
func (p *Cache) Mock(a heptane.CacheAccess, err error) {
	switch a := a.(type) {
	case heptane.CacheGet:
		p.s = append(p.s, access{a, err})
	case *heptane.CacheGet:
		p.s = append(p.s, access{*a, err})
	case heptane.CacheSet:
		p.s = append(p.s, access{a, err})
	case *heptane.CacheSet:
		p.s = append(p.s, access{*a, err})
	}
}

// Access implements CacheProvider.
func (p *Cache) Access(a heptane.CacheAccess) error {
	switch a := a.(type) {
	case *heptane.CacheGet:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case heptane.CacheGet:
				if g.Key != a.Key {
					continue
				}
				a.Value = g.Value
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case heptane.CacheSet:
		for _, b := range p.s {
			switch s := b.a.(type) {
			case heptane.CacheSet:
				if s.Key != a.Key {
					continue
				}
				if !bytes.Equal(s.Value, a.Value) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *heptane.CacheSet:
		return p.Access(*a)
	}
	return fmt.Errorf("Unsupported heptane.CacheAccess Type: %T", a)
}

// AccessSlice implements CacheProvider.
func (p *Cache) AccessSlice(aa []heptane.CacheAccess) (errs []error) {
	for _, a := range aa {
		errs = append(errs, p.Access(a))
	}
	return
}
