package heptane

import (
	"fmt"

	r "github.com/heptanes/heptane/row"
)

// UnsupportedRowAccessTypeError is produced when the type of a RowAccess is
// not supported. Current supported types are RowCreate, RowRetrieve, RowUpdate
// and RowDelete.
type UnsupportedRowAccessTypeError struct {
	RowAccess r.RowAccess
}

func (e UnsupportedRowAccessTypeError) Error() string {
	return fmt.Sprintf("Unsupported RowAccess Type: %#v", e.RowAccess)
}

// SqlError is produced when the package database.sql returns an error.
type SqlError struct {
	Err error
}

func (e SqlError) Error() string {
	return fmt.Sprintf("Sql Error: %v", e.Err)
}
