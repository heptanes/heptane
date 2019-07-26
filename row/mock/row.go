package heptane

import (
	"fmt"

	r "github.com/heptanes/heptane/row"
)

type access struct {
	a r.RowAccess
	e error
}

// Row implements RowProvider.
type Row struct {
	s []access
}

// Mock ensures the following calls to Access() with the given input fields of
// RowAccess will return the given output fields of RowAccess and the given
// error.
func (p *Row) Mock(a r.RowAccess, err error) {
	switch a := a.(type) {
	case r.RowCreate:
		p.s = append(p.s, access{a, err})
	case *r.RowCreate:
		p.s = append(p.s, access{*a, err})
	case r.RowRetrieve:
		p.s = append(p.s, access{a, err})
	case *r.RowRetrieve:
		p.s = append(p.s, access{*a, err})
	case r.RowUpdate:
		p.s = append(p.s, access{a, err})
	case *r.RowUpdate:
		p.s = append(p.s, access{*a, err})
	case r.RowDelete:
		p.s = append(p.s, access{a, err})
	case *r.RowDelete:
		p.s = append(p.s, access{*a, err})
	}
}

// Access implements RowProvider.
func (p *Row) Access(a r.RowAccess) error {
	switch a := a.(type) {
	case r.RowCreate:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case r.RowCreate:
				if fmt.Sprintf("%#v", g.Table) != fmt.Sprintf("%#v", a.Table) {
					continue
				}
				if fmt.Sprintf("%#v", g.FieldValues) != fmt.Sprintf("%#v", a.FieldValues) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *r.RowCreate:
		return p.Access(*a)
	case *r.RowRetrieve:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case r.RowRetrieve:
				if fmt.Sprintf("%#v", g.Table) != fmt.Sprintf("%#v", a.Table) {
					continue
				}
				if fmt.Sprintf("%#v", g.FieldValues) != fmt.Sprintf("%#v", a.FieldValues) {
					continue
				}
				a.RetrievedValues = g.RetrievedValues
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case r.RowUpdate:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case r.RowUpdate:
				if fmt.Sprintf("%#v", g.Table) != fmt.Sprintf("%#v", a.Table) {
					continue
				}
				if fmt.Sprintf("%#v", g.FieldValues) != fmt.Sprintf("%#v", a.FieldValues) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *r.RowUpdate:
		return p.Access(*a)
	case r.RowDelete:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case r.RowDelete:
				if fmt.Sprintf("%#v", g.Table) != fmt.Sprintf("%#v", a.Table) {
					continue
				}
				if fmt.Sprintf("%#v", g.FieldValues) != fmt.Sprintf("%#v", a.FieldValues) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *r.RowDelete:
		return p.Access(*a)
	}
	return fmt.Errorf("Unsupported heptane.RowAccess Type: %T", a)
}

// AccessSlice implements RowProvider.
func (p *Row) AccessSlice(aa []r.RowAccess) (errs []error) {
	for _, a := range aa {
		errs = append(errs, p.Access(a))
	}
	return
}
