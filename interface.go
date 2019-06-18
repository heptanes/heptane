package heptane

type TableName string

type FieldName string

type FieldType string

type TablePrimaryKeyCache struct {
	Enabled      bool   `json:"enabled"`
	CachePrefix  string `json:"cachePrefix"`
	CacheVersion uint   `json:"cacheVersion"`
}

type Table struct {
	Name            string                  `json:"name"`
	Fields          map[FieldName]FieldType `json:"fields"`
	PrimaryKey      []FieldName             `json:"primaryKey"`
	PartitionKey    []FieldName             `json:"partitionKey"`
	PrimaryKeyCache TablePrimaryKeyCache    `json:"primaryKeyCache"`
}
