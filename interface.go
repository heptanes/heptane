package heptane

// TableName is the name of a table.
type TableName string

// FieldName is the name of a field of a table.
type FieldName string

// FieldType is the type of the values of a field of a table.
type FieldType string

// FieldTypesByName is a map from FieldName to FieldTypes.
type FieldTypesByName map[FieldName]FieldType

// FieldValue is the value of a field of a table.
type FieldValue interface{}

// FieldValuesByName is a map from FieldName to FieldValues. It represents a
// primary key, a partition key, a full row of a table, and so on.
type FieldValuesByName map[FieldName]FieldValue

// CacheKey is the key of a cache entry.
type CacheKey string

// CacheValue is the value of a cache entry. A nil slice means a cache miss, a
// non nil slice means a cache hit even if it is empty.
type CacheValue []byte

// Table is the specification of a table.
type Table struct {
	// Name is the name of the table.
	Name TableName `json:"name"`
	// PartitionKey specifies the names of all the fields in the partition
	// key of the table. The order in the slice matters.
	PartitionKey []FieldName `json:"partitionKey"`
	// PrimaryKey specifies the names of all the fields in the primary key
	// of the table. It must contain the PartitionKey as prefix. The order
	// in the slice matters.
	PrimaryKey []FieldName `json:"primaryKey"`
	// Values specifies the names of all the fields that are not in the
	// primary key. The order in the slice matters.
	Values []FieldName `json:"values"`
	// Types specifies the types of all the fields of the table.
	Types FieldTypesByName `json:"types"`
	// PrimaryKeyCachePrefix is the prefix of all the keys in the primary
	// key cache of the table. Each row in the table has the primary key as
	// CacheKey and the remaining fields as CacheValue. The primary key is
	// enabled if and only if the PrimaryKeyCachePrefix is not null. Users
	// probably want to set a string that identifies a table and a version
	// of the contents of the cache.
	PrimaryKeyCachePrefix []CacheKey `json:"primaryKeyCachePrefix"`
}

// RowAccess is the interface of all types that represent an access to a table.
type RowAccess interface{}

// RowCreate specifies the creation of a row in a table.
type RowCreate struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key and the values.
	FieldValues FieldValuesByName
}

// RowRetrieve specifies the retrieval of one or several rows in a table.
type RowRetrieve struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the partition key and optionally other fields
	// from the primary key.
	FieldValues FieldValuesByName
	// FieldValues will contain one or more rows, each one with all its
	// fields.
	RetrievedValues []FieldValuesByName
}

// RowUpdate specifies the update of a row in a table.
type RowUpdate struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key and the values.
	FieldValues FieldValuesByName
}

// RowDelete specifies the deletion of a row in a table.
type RowDelete struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
}

// RowProvider is the interface of all implementations that access tables
// directly.
type RowProvider interface {
	// Access performs the given acccess to the table.
	Access(RowAccess) error
	// AccessSlice performs several acccesses to the table.
	AccessSlice([]RowAccess) []error
}

// CacheAccess is the interface of all types that represent an access to a
// cache.
type CacheAccess interface{}

// CacheGet specifies the retrieval of a cache entry.
type CacheGet struct {
	// CacheKey is the key of the cache entry.
	Key CacheKey
	// CacheVlaue will contain the value of the cache entry.
	Value CacheValue
}

// CacheSet specifies the creation or update of a cache entry.
type CacheSet struct {
	// CacheKey is the key of the cache entry.
	Key CacheKey
	// CacheVlaue is the value of the cache entry.
	Value CacheValue
}

// CacheProvider is the interface of all implementations that access caches
// directly.
type CacheProvider interface {
	// Access performs the given acccess to the cache.
	Access(CacheAccess) error
	// AccessSlice performs several acccesses to the cache.
	AccessSlice([]CacheAccess) []error
}

// Access is the interface of all types that represent an access to a table
// using a cache.
type Access interface{}

// Create specifies the creation of a row in a table.
type Create struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key and the values.
	FieldValues FieldValuesByName
}

// Retrieve specifies the retrieval of one or several rows in a table.
type Retrieve struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the partition key and optionally other fields
	// from the primary key.
	FieldValues FieldValuesByName
	// FieldValues will contain one or more rows, each one with all its
	// fields.
	RetrievedValues []FieldValuesByName
}

// Update specifies the update of a row in a table.
type Update struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key and the values.
	FieldValues FieldValuesByName
}

// Delete specifies the deletion of a row in a table.
type Delete struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
}

// Heptane is the main interface, it provides a uniform access to tables
// supported by different RowProviders and CacheProviders. Safe to be used from
// different goroutines.
type Heptane interface {
	// Register creates and updates a mapping between a TableName and its
	// specification: the Table, RowProvider and CacheProvider.
	Register(Table, RowProvider, CacheProvider)
	// Unregister deletes the mapping between the given Tablename and its
	// specification.
	Unregister(TableName)
	// TableNames returns a slice of all names of tables that have been
	// registered.
	TableNames() []TableName
	// Table returns the current associated Table of the given TableName.
	Table(TableName) Table
	// RowProvider the current associated RowProvider of the given
	// TableName.
	RowProvider(TableName) RowProvider
	// CacheProvider the current associated CacheProvider of the given
	// TableName.
	CacheProvider(TableName) CacheProvider
	// Access performs the given acccess to the table using the cache.
	Access(Access) error
	// AccessSlice performs several acccesses to the table using the cache.
	AccessSlice([]Access) []error
}
