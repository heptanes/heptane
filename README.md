HEPTANE - cacHEd disPerse Table bAckeNd framEwork
=================================================

Heptane is a framework for development of backend solutions that store data in disperse databases:

* It provides a uniform API to access the data on disperse tables.
* It provides several implementations of the given API using different providers for the tables and the cache, i.e. cassandra, hbase, dynamodb, sharding over a cluster of relational databases, memcached, redis, redis cluster.
* It provides development tools.

The API is defined with the following goals:

* Only the common operations to all disperse database providers are implemented.
* Different database and cache providers can be used at the same time.
* High enphasis on the parallel access to the database and cache.

In detail:

* Users define disperse table metadata: name, fields, primary key, partition key, cache policies.
* The framework provides an API to write data into the tables specified by the metadata, updating the cache at the same time.
* The framework provides an API to read data from the tables specified by the metadata, using the cache.
* The framework takes care of the particular implementation details of each db provider.

The framework provides the following development tools:

* Given table metadata, the framework is able to create the tables in the database provider.
* Given an existing schema in the database provide, the framework is able to extract the table metadata.
* The framework is able to migrate data between different database providers.

Installation
------------

Using `go get` as usual:

* `go get github.com/heptanes/heptane`
* `import "github.com/heptanes/heptane"`

