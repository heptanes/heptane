package heptane

import (
	"database/sql"
	"strings"

	r "github.com/heptanes/heptane/row"
)

// Dialect offers methods to generate database specific sql strings.
type Dialect interface {

	// TableName writes the name of a table in a sql string to the Builder.
	WriteTableName(sb *strings.Builder, n r.TableName)

	// FieldName writes the name of a field in a sql string to the Builder.
	WriteFieldName(sb *strings.Builder, n r.FieldName)

	// Placeholder writes a placeholder for the argument i of a sql string to
	// the Builder.
	WritePlaceholder(sb *strings.Builder, i int)
}

// Row implements RowProvider. Each RowAccess is performed on a single sql.DB.
type Row struct {
	DB      *sql.DB
	Dialect Dialect
}

func (p *Row) exec(query string, args ...interface{}) (err error) {
	if _, err = p.DB.Exec(query, args...); err != nil {
		err = SqlError{err}
		return
	}
	return
}

func (p *Row) query(b r.Table, query string, args ...interface{}) (fnvs []r.FieldValuesByName, err error) {
	rows, err := p.DB.Query(query, args...)
	if err != nil {
		err = SqlError{err}
		return
	}
	defer rows.Close()
	for rows.Next() {
		scan := make([]interface{}, len(b.Values))
		for i, fn := range b.Values {
			ft := b.Types[fn]
			switch ft {
			case "bool":
				// TODO allow nulls
				scan[i] = new(bool)
			default: // "string"
				// TODO allow nulls
				scan[i] = new(string)
			}
		}
		if err = rows.Scan(scan...); err != nil {
			err = SqlError{err}
			return
		}
		fvn := r.FieldValuesByName{}
		for i, v := range scan {
			fn := b.Values[i]
			ft := b.Types[fn]
			switch ft {
			case "bool":
				// TODO check nulls
				fvn[fn] = *v.(*bool)
			default: // "string"
				// TODO check nulls
				fvn[fn] = *v.(*string)
			}
		}
		fnvs = append(fnvs, fvn)
	}
	if err = rows.Err(); err != nil {
		err = SqlError{err}
		return
	}
	return
}

func (p *Row) Create(a r.RowCreate) error {
	if err := a.Table.Validate(); err != nil {
		return err
	}
	sb := &strings.Builder{}
	sb.WriteString("INSERT INTO ")
	p.Dialect.WriteTableName(sb, a.Table.Name)
	sb.WriteString(" (")
	for i, fn := range a.Table.PrimaryKey {
		if i != 0 {
			sb.WriteString(", ")
		}
		p.Dialect.WriteFieldName(sb, fn)
	}
	for _, fn := range a.Table.Values {
		_, ok := a.FieldValues[fn]
		if ok {
			sb.WriteString(", ")
			p.Dialect.WriteFieldName(sb, fn)
		}
	}
	sb.WriteString(") VALUES (")
	args := make([]interface{}, 0, len(a.Table.PrimaryKey)+len(a.Table.Values))
	for i, fn := range a.Table.PrimaryKey {
		if i != 0 {
			sb.WriteString(", ")
		}
		p.Dialect.WritePlaceholder(sb, i)
		fv := a.FieldValues[fn]
		args = append(args, fv)
	}
	for _, fn := range a.Table.Values {
		fv, ok := a.FieldValues[fn]
		if ok {
			sb.WriteString(", ")
			p.Dialect.WritePlaceholder(sb, len(args))
			args = append(args, fv)
		}
	}
	sb.WriteString(")")
	return p.exec(sb.String(), args...)
}

func (p *Row) Retrieve(a *r.RowRetrieve) error {
	if err := a.Table.Validate(); err != nil {
		return err
	}
	sb := &strings.Builder{}
	sb.WriteString("SELECT ")
	for i, fn := range a.Table.Values {
		if i != 0 {
			sb.WriteString(", ")
		}
		p.Dialect.WriteFieldName(sb, fn)
	}
	sb.WriteString(" FROM ")
	p.Dialect.WriteTableName(sb, a.Table.Name)
	sb.WriteString(" WHERE ")
	args := make([]interface{}, 0, len(a.Table.PrimaryKey))
	for i, fn := range a.Table.PrimaryKey {
		if i != 0 {
			sb.WriteString(" AND ")
		}
		p.Dialect.WriteFieldName(sb, fn)
		fv := a.FieldValues[fn]
		if fv == nil {
			sb.WriteString(" IS NULL")
			continue
		}
		sb.WriteString(" = ")
		p.Dialect.WritePlaceholder(sb, i)
		args = append(args, fv)
	}
	fvn, err := p.query(a.Table, sb.String(), args...)
	a.RetrievedValues = fvn
	return err
}

func (p *Row) Update(a r.RowUpdate) error {
	if err := a.Table.Validate(); err != nil {
		return err
	}
	sb := &strings.Builder{}
	sb.WriteString("UPDATE ")
	p.Dialect.WriteTableName(sb, a.Table.Name)
	sb.WriteString(" SET ")
	args := make([]interface{}, 0, len(a.Table.PrimaryKey)+len(a.Table.Values))
	for i, fn := range a.Table.Values {
		fv, ok := a.FieldValues[fn]
		if ok {
			if i != 0 {
				sb.WriteString(", ")
			}
			p.Dialect.WriteFieldName(sb, fn)
			sb.WriteString(" = ")
			p.Dialect.WritePlaceholder(sb, len(args))
			args = append(args, fv)
		}
	}
	sb.WriteString(" WHERE ")
	for i, fn := range a.Table.PrimaryKey {
		if i != 0 {
			sb.WriteString(" AND ")
		}
		p.Dialect.WriteFieldName(sb, fn)
		fv := a.FieldValues[fn]
		if fv == nil {
			sb.WriteString(" IS NULL")
			continue
		}
		sb.WriteString(" = ")
		p.Dialect.WritePlaceholder(sb, i)
		args = append(args, fv)
	}
	return p.exec(sb.String(), args...)
}

func (p *Row) Delete(a r.RowDelete) error {
	if err := a.Table.Validate(); err != nil {
		return err
	}
	sb := &strings.Builder{}
	sb.WriteString("DELETE FROM ")
	p.Dialect.WriteTableName(sb, a.Table.Name)
	sb.WriteString(" WHERE ")
	args := make([]interface{}, 0, len(a.Table.PrimaryKey))
	for i, fn := range a.Table.PrimaryKey {
		if i != 0 {
			sb.WriteString(" AND ")
		}
		p.Dialect.WriteFieldName(sb, fn)
		fv := a.FieldValues[fn]
		if fv == nil {
			sb.WriteString(" IS NULL")
			continue
		}
		sb.WriteString(" = ")
		p.Dialect.WritePlaceholder(sb, i)
		args = append(args, fv)
	}
	return p.exec(sb.String(), args...)
}

// Access implements RowProvider.
func (p *Row) Access(a r.RowAccess) error {
	switch a := a.(type) {
	case r.RowCreate:
		return p.Create(a)
	case *r.RowCreate:
		return p.Create(*a)
	case *r.RowRetrieve:
		return p.Retrieve(a)
	case r.RowUpdate:
		return p.Update(a)
	case *r.RowUpdate:
		return p.Update(*a)
	case r.RowDelete:
		return p.Delete(a)
	case *r.RowDelete:
		return p.Delete(*a)
	}
	return UnsupportedRowAccessTypeError{a}
}

// AccessSlice implements RowProvider.
func (p *Row) AccessSlice(aa []r.RowAccess) (errs []error) {
	for _, a := range aa {
		errs = append(errs, p.Access(a))
	}
	return
}
