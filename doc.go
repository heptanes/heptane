/*
HEPTANE - cacHEd disPerse Table bAckeNd framEwork.

This package provides a main interface Heptane and one implementation
accessible from the constructor New().

Heptane maintains metadata about a set of tables (an instance of Table, an
instance of RowProvider and an optional instance of CacheProvider per table)
and provides operations to access the contents of these tables.

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


RowProviders And CacheProviders

Operations are performed on TableNames, then Heptane looks for the registered
RowProvider and CacheProvider for that TableName, performs the action on the
underlying table through the RowProvider and synchronizes the PrimaryKey cache
through the CacheProvider.

RowProviders offer access to specific databases. The definition of a Table is
that of a disperse table, databases that implement disperse tables are a
natural fit for a RowProvider implementation. The API is simple enough that
RowProvider implementations for relational databases are possible too.

CacheProviders are simple key value pairs that need to provide only the
operations Set and Get. Values are stored without expiration time.

Accesses

The type Access is the interface for all operations, and there is one struct
for each operation: Create, Retrieve, Update and Delete.

Create sends a RowCreate operation to the RowProvider and, if successful, sends
a CacheSet operation to the CacheProvider.

When the full PrimaryKey is given, Retrieve sends a CacheGet operation to the
CacheProvider. The row is returned if found, otherwise it sends a RowRetrieve
operation to the RowProvider, and then sends a CacheSet operation to the
CacheProvider.

When the full PrimaryKey is not given, but at least the full PartitionKey must
be given, Retrieve sends a RowRetrieve operation to the RowProvider, and then
sends a CacheSet operation to the CacheProvider for each retrieved row.

When the full Values are given, Update sends a RowUpdate operation to the
RowProvider and, if successful, sends a CacheSet operation to the
CacheProvider.

When the full Values are not given, Update sends the partial RowUpdate
operation to the RowProvider and, if successful, sends a RowRetrieve to the
RowProvider and, if successful, it sends a CacheSet operation to the
CacheProvider.

Delete sends a RowDelete operation to the RowProvider and, if successful, sends
a CacheSet operation to the CacheProvider. The value stored in the cache means
that there is no row in the table, so the next Retrieve does not need to query
the RowProvider.

Retrieve must be passed as reference so the RetrievedValues set by Heptane may
be read by the client code.
*/
package heptane
