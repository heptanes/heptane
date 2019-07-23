package heptane

import (
	"fmt"

	"github.com/heptanes/heptane"
)

type access struct {
	a heptane.RowAccess
	e error
}

// Row implements RowProvider.
type Row struct {
	s []access
}

// Mock ensures the following calls to Access() with the given input fields of
// RowAccess will return the given output fields of RowAccess and the given
// error.
func (p *Row) Mock(a heptane.RowAccess, err error) {
	switch a := a.(type) {
	case heptane.RowCreate:
		p.s = append(p.s, access{a, err})
	case *heptane.RowCreate:
		p.s = append(p.s, access{*a, err})
	case heptane.RowRetrieve:
		p.s = append(p.s, access{a, err})
	case *heptane.RowRetrieve:
		p.s = append(p.s, access{*a, err})
	case heptane.RowUpdate:
		p.s = append(p.s, access{a, err})
	case *heptane.RowUpdate:
		p.s = append(p.s, access{*a, err})
	case heptane.RowDelete:
		p.s = append(p.s, access{a, err})
	case *heptane.RowDelete:
		p.s = append(p.s, access{*a, err})
	}
}

// Access implements RowProvider.
func (p *Row) Access(a heptane.RowAccess) error {
	switch a := a.(type) {
	case heptane.RowCreate:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case heptane.RowCreate:
				if fmt.Sprint(g.Table) != fmt.Sprint(a.Table) {
					continue
				}
				if fmt.Sprint(g.FieldValues) != fmt.Sprint(a.FieldValues) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *heptane.RowCreate:
		return p.Access(*a)
	case *heptane.RowRetrieve:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case heptane.RowRetrieve:
				if fmt.Sprint(g.Table) != fmt.Sprint(a.Table) {
					continue
				}
				if fmt.Sprint(g.FieldValues) != fmt.Sprint(a.FieldValues) {
					continue
				}
				a.RetrievedValues = g.RetrievedValues
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case heptane.RowUpdate:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case heptane.RowUpdate:
				if fmt.Sprint(g.Table) != fmt.Sprint(a.Table) {
					continue
				}
				if fmt.Sprint(g.FieldValues) != fmt.Sprint(a.FieldValues) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *heptane.RowUpdate:
		return p.Access(*a)
	case heptane.RowDelete:
		for _, b := range p.s {
			switch g := b.a.(type) {
			case heptane.RowDelete:
				if fmt.Sprint(g.Table) != fmt.Sprint(a.Table) {
					continue
				}
				if fmt.Sprint(g.FieldValues) != fmt.Sprint(a.FieldValues) {
					continue
				}
				return b.e
			}
		}
		return fmt.Errorf("Not Mocked: %#v", a)
	case *heptane.RowDelete:
		return p.Access(*a)
	}
	return fmt.Errorf("Unsupported heptane.RowAccess Type: %T", a)
}

// AccessSlice implements RowProvider.
func (p *Row) AccessSlice(aa []heptane.RowAccess) (errs []error) {
	for _, a := range aa {
		errs = append(errs, p.Access(a))
	}
	return
}
