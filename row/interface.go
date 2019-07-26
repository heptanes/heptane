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
	PrimaryKeyCachePrefix []string `json:"primaryKeyCachePrefix"`
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
