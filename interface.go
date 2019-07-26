package heptane

import (
	c "github.com/heptanes/heptane/cache"
	r "github.com/heptanes/heptane/row"
)

// Access is the interface of all types that represent an access to a table
// using a cache.
type Access interface{}

// Create specifies the creation of a row in a table.
type Create struct {
	// TableName is the name of the table.
	TableName r.TableName
	// FieldValues contains the primary key and the values.
	FieldValues r.FieldValuesByName
}

// Retrieve specifies the retrieval of one or several rows in a table.
type Retrieve struct {
	// TableName is the name of the table.
	TableName r.TableName
	// FieldValues contains the partition key and optionally other fields
	// from the primary key.
	FieldValues r.FieldValuesByName
	// FieldValues will contain one or more rows, each one with all its
	// fields.
	RetrievedValues []r.FieldValuesByName
}

// Update specifies the update of a row in a table.
type Update struct {
	// TableName is the name of the table.
	TableName r.TableName
	// FieldValues contains the primary key and the values.
	FieldValues r.FieldValuesByName
}

// Delete specifies the deletion of a row in a table.
type Delete struct {
	// TableName is the name of the table.
	TableName r.TableName
	// FieldValues contains the primary key.
	FieldValues r.FieldValuesByName
}

// Heptane is the main interface, it provides a uniform access to tables
// supported by different RowProviders and CacheProviders. Safe to be used from
// different goroutines.
type Heptane interface {
	// Register creates and updates a mapping between a TableName and its
	// specification: the Table, RowProvider and CacheProvider.
	Register(r.Table, r.RowProvider, c.CacheProvider) error
	// Unregister deletes the mapping between the given Tablename and its
	// specification.
	Unregister(r.TableName)
	// TableNames returns a slice of all names of tables that have been
	// registered.
	TableNames() []r.TableName
	// Table returns the current associated Table of the given TableName.
	Table(r.TableName) r.Table
	// RowProvider the current associated RowProvider of the given
	// TableName.
	RowProvider(r.TableName) r.RowProvider
	// CacheProvider the current associated CacheProvider of the given
	// TableName.
	CacheProvider(r.TableName) c.CacheProvider
	// Access performs the given acccess to the table using the cache.
	Access(Access) error
	// AccessSlice performs several acccesses to the table using the cache.
	AccessSlice([]Access) []error
}
