/*
Interface definition of RowProvider.

RowProviders offer access to specific databases. The definition of a Table is
that of a disperse table, databases that implement disperse tables are a
natural fit for a RowProvider implementation. The API is simple enough that
RowProvider implementations for relational databases are possible too.

The Table

The type Table describes the underlying physical table:

the Name,

the names of the fields that compose the PartitionKey (there must be at least
one field),

the names of the fields that compose the PrimaryKey (the PrimaryKey must
contain the PartitionKey as prefix),

the names of the fields that compose the Values (there may be no fields at
all),

the types of each field in a map FieldTypesByName (strings and bools),

the prefix PrimaryKeyCachePrefix in an external cache that contains the
PrimaryKeys as cache key and the Values as cache values.

RowAccesses

RowThe type Access is the interface for all operations, and there is one struct
for each operation: RowCreate, RowRetrieve, RowUpdate and RowDelete.

RowCreate means an INSERT into the table. Full PrimaryKey is mandatory, Values
are optional, only given Values are inserted.

RowRetrieve means a SELECT from the table of all Values. For the WHERE: full
PartitionKey is mandatory, fields from the PrimaryKey are optional, Values are
ignored.

RowUpdate means an UPDATE of the table. Full PrimaryKey is mandatory, Values
are optional, only given Values are updated.

RowDelete means a DELETE from the table. Full PrimaryKey is mandatory.

RowRetrieve must be passed as reference so the RetrievedValues set by the
RowProvider may be read by the client code.
*/
package heptane
