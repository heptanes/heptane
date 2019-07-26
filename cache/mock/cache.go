package heptane

import (
	"fmt"

	c "github.com/heptanes/heptane/cache"
)

type access struct {
	a c.CacheAccess
	e error
}

// Cache implements CacheProvider.
type Cache struct {
	s []access
}

// Mock ensures the following calls to Access() with the given input fields of
// CacheAccess will return the given output fields of CacheAccess and the given
// error.
func (p *Cache) Mock(a c.CacheAccess, err error) {
	switch a := a.(type) {
	case c.CacheGet:
		p.s = append(p.s, access{a, err})
	case *c.CacheGet:
		p.s = append(p.s, access{*a, err})
	case c.CacheSet:
		p.s = append(p.s, access{a, err})
	case *c.CacheSet:
		p.s = append(p.s, access{*a, err})
	}
}

// Access implements CacheProvider.
func (p *Cache) Access(a c.CacheAccess) error {
	switch a := a.(type) {
	case *c.CacheGet:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case c.CacheGet:
				if g.Key != a.Key {
					continue
				}
				a.Value = g.Value
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case c.CacheSet:
		for _, b := range p.s {
			switch s := b.a.(type) {
			case c.CacheSet:
				if s.Key != a.Key {
					continue
				}
				if fmt.Sprintf("%#v", s.Value) != fmt.Sprintf("%#v", a.Value) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *c.CacheSet:
		return p.Access(*a)
	}
	return fmt.Errorf("Unsupported heptane.CacheAccess Type: %T", a)
}

// AccessSlice implements CacheProvider.
func (p *Cache) AccessSlice(aa []c.CacheAccess) (errs []error) {
	for _, a := range aa {
		errs = append(errs, p.Access(a))
	}
	return
}
