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

// CacheKey is a slice a strings that joined together compose the key of a
// cache entry.
type CacheKey []string

// CacheValue is the value of a cache entry.
type CacheValue string

// Table is the specification of a table.
type Table struct {
	// Name is the name of the table.
	Name TableName `json:"name"`
	// Fields specifies the types of all the fields of the table.
	Fields FieldTypesByName `json:"fields"`
	// PrimaryKey specifies the names of all the fields in the primary key
	// of the table.
	PrimaryKey []FieldName `json:"primaryKey"`
	// PartitionKey specifies the names of all the fields in the partition
	// key of the table.
	PartitionKey []FieldName `json:"partitionKey"`
	// PrimaryKeyCachePrefix is the prefix of all the keys in the primary
	// key cache of the table. Each row in the table has the primary key as
	// CacheKey and the remaining fields as CacheValue. The primary key is
	// enabled if and only if the PrimaryKeyCachePrefix is not null. Users
	// probably want to set a string that identifies a table and a version
	// of the contents of the cache.
	PrimaryKeyCachePrefix CacheKey `json:"primaryKeyCachePrefix"`
}

// TableAccess is the interface of all types that represent an access to a
// table.
type TableAccess interface{}

// TableCreate specifies the creation of a row in a table.
type TableCreate struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
}

// TableUpdate specifies the update of a row in a table.
type TableUpdate struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key and the values to be updated.
	FieldValues FieldValuesByName
}

// TableRetrieve specifies the retrieval of a row in a table.
type TableRetrieve struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
	// FieldValues will contain the values retrieved from the table.
	RetrievedValues FieldValuesByName
}

// TableDelete specifies the deletion of a row in a table.
type TableDelete struct {
	// Table is the specification of the table.
	Table Table
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
}

// TableProvider is the interface of all implementations that access tables
// directly.
type TableProvider interface {
	// Access performs the given acccess to the table.
	Access(TableAccess) error
	// AccessSlice performs several acccesses to the table.
	AccessSlice([]TableAccess) []error
}

// CacheAccess is the interface of all types that represent an access to a
// cache.
type CacheAccess interface{}

// CacheGet specifies the retrieval of a cache entry.
type CacheGet struct {
	// CacheKey is the key of the cache entry.
	Key CacheKey
	// CacheVlaue is the value of the cache entry.
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

// Access is the interface of all types that represent an access to a
// table using a cache.
type Access interface{}

// Create specifies the creation of a row in a table.
type Create struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
}

// Update specifies the update of a row in a table.
type Update struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key and the values to be updated.
	FieldValues FieldValuesByName
}

// Retrieve specifies the retrieval of a row in a table.
type Retrieve struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
	// FieldValues will contain the values retrieved from the table.
	RetrievedValues FieldValuesByName
}

// Delete specifies the deletion of a row in a table.
type Delete struct {
	// TableName is the name of the table.
	TableName TableName
	// FieldValues contains the primary key.
	FieldValues FieldValuesByName
}

// Heptane is the main interface, it provides a uniform access to tables
// supported by different TableProviders and CacheProviders. Safe to be used
// from different goroutines.
type Heptane interface {
	// Register creates and updates a mapping between a TableName and its
	// specification: the Table, TableProvider and CacheProvider.
	Register(Table, TableProvider, CacheProvider) error
	// Unregister deletes the mapping between the given Tablename and its
	// specification.
	Unregister(TableName) error
	// TableNames returns a slice of all names of tables that have been registered.
	TableNames() []TableName
	// Table returns the current associated Table of the given TableName.
	Table(TableName) Table
	// TableProvider the current associated TableProvider of the given
	// TableName.
	TableProvider(TableName) TableProvider
	// CacheProvider the current associated CacheProvider of the given
	// TableName.
	CacheProvider(TableName) CacheProvider
	// Access performs the given acccess to the table using the cache.
	Access(Access) error
	// AccessSlice performs several acccesses to the table using the cache.
	AccessSlice([]Access) []error
}
