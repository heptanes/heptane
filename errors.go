package heptane

import (
	"fmt"

	c "github.com/heptanes/heptane/cache"
	r "github.com/heptanes/heptane/row"
)

// UnregisteredTableError is produced when an Access is tried on a
// TableName that has not been registered.
type UnregisteredTableError struct {
	TableName r.TableName
}

func (e UnregisteredTableError) Error() string {
	return fmt.Sprintf("Unregistered Table %v", e.TableName)
}

// NullRowProviderError is produced when a Table is registered with a nil
// RowProvider.
type NullRowProviderError struct {
	TableName r.TableName
}

func (e NullRowProviderError) Error() string {
	return fmt.Sprintf("Null RowProvider for Table %v", e.TableName)
}

// RowProviderAccessError is produced when a RowProvider returns an error for a
// given RowAccess.
type RowProviderAccessError struct {
	Access r.RowAccess
	Err    error
}

func (e RowProviderAccessError) Error() string {
	return fmt.Sprintf("%#v Error: %v", e.Access, e.Err)
}

// CacheProviderAccessError is produced when a CacheProvider returns an error
// for a given CacheAccess.
type CacheProviderAccessError struct {
	Access c.CacheAccess
	Err    error
}

func (e CacheProviderAccessError) Error() string {
	return fmt.Sprintf("%#v Error: %v", e.Access, e.Err)
}

// UnsupportedAccessTypeError is produced when the type of an Access is not
// supported. Current supported types are Create, Retrieve, Update and Delete.
type UnsupportedAccessTypeError struct {
	Access Access
}

func (e UnsupportedAccessTypeError) Error() string {
	return fmt.Sprintf("Unsupported Access Type: %#v", e.Access)
}

// UnsupportedFieldValueError is produced when the type of a FieldValue does
// not match the corresponding FieldType.
type UnsupportedFieldValueError struct {
	FieldType  r.FieldType
	FieldValue r.FieldValue
}

func (e UnsupportedFieldValueError) Error() string {
	return fmt.Sprintf("Unsupported FieldValue for FieldType %v: %v", e.FieldType, e.FieldValue)
}

// MissingFieldValueError is produced when a Table defines a FieldName that has
// no value in a FieldValuesByName.
type MissingFieldValueError struct {
	TableName         r.TableName
	FieldName         r.FieldName
	FieldValuesByName r.FieldValuesByName
}

func (e MissingFieldValueError) Error() string {
	return fmt.Sprintf("Missing FieldValue for Field %v.%v: %v", e.TableName, e.FieldName, e.FieldValuesByName)
}

// MultipleErrors encapsulates one or more errors typically produced
// concurrently.
type MultipleErrors struct {
	Errors []error
}

func (e MultipleErrors) Error() string {
	return fmt.Sprintf("Multiple Errors: %v", e.Errors)
}
