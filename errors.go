package heptane

import "fmt"

// UnregisteredTableNameError is produced when an Access is tried on a
// TableName that has not been registered.
type UnregisteredTableNameError struct {
	TableName TableName
}

func (e UnregisteredTableNameError) Error() string {
	return fmt.Sprintf("Unregistered TableName %v", e.TableName)
}

// UnregisteredRowProviderError is produced when an Access is tried on a
// TableName that has been registered but its RowProvider is nil.
type UnregisteredRowProviderError struct {
	TableName TableName
}

func (e UnregisteredRowProviderError) Error() string {
	return fmt.Sprintf("Unregistered RowProvider for TableName %v", e.TableName)
}

// RowProviderAccessError is produced when a RowProvider returns an error for a
// given RowAccess.
type RowProviderAccessError struct {
	Access RowAccess
	Err    error
}

func (e RowProviderAccessError) Error() string {
	return fmt.Sprintf("%#v Error: %v", e.Access, e.Err)
}

// CacheProviderAccessError is produced when a CacheProvider returns an error
// for a given CacheAccess.
type CacheProviderAccessError struct {
	Access CacheAccess
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

// UnsupportedFieldTypeError is produced when a FieldType is not supported.
// Current supported values are "string".
type UnsupportedFieldTypeError struct {
	FieldType FieldType
}

func (e UnsupportedFieldTypeError) Error() string {
	return fmt.Sprintf("Unsupported FieldType: %v", e.FieldType)
}

// UnsupportedFieldValueError is produced when the type of a FieldValue does
// not match the corresponding FieldType.
type UnsupportedFieldValueError struct {
	FieldType  FieldType
	FieldValue FieldValue
}

func (e UnsupportedFieldValueError) Error() string {
	return fmt.Sprintf("Unsupported FieldValue for FieldType %v: %v", e.FieldType, e.FieldValue)
}

// UndefinedFieldTypeError is produced when the a FieldType has not been found
// in the FieldTypesByName for a given FieldName.
type UndefinedFieldTypeError struct {
	TableName TableName
	FieldName FieldName
}

func (e UndefinedFieldTypeError) Error() string {
	return fmt.Sprintf("Undefined FieldType for %v.%v", e.TableName, e.FieldName)
}

// UndefinedFieldValueError is produced when a mandatory FieldValue has not
// been found for a given FieldName, i.e. a field for a primary key.
type UndefinedFieldValueError struct {
	FieldName         FieldName
	FieldValuesByName FieldValuesByName
}

func (e UndefinedFieldValueError) Error() string {
	return fmt.Sprintf("Undefined FieldValue for %v in %v", e.FieldName, e.FieldValuesByName)
}

// MultipleErrors encapsulates one or more errors typically produced
// concurrently.
type MultipleErrors struct {
	Errors []error
}

func (e MultipleErrors) Error() string {
	return fmt.Sprintf("Multiple Errors: %v", e.Errors)
}
