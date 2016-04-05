# Cassandra FHIR Index

Cassandra `Index` implementation for [FHIR® – Fast Healthcare Interoperability Resources](http://hl7.org/fhir/index.html). The index provides near real-time search capabilities and full-support (excluding chained and composite search parameters) for search operations defined in [FHIR Search Framework](http://hl7-fhir.github.io/search.html).

Cassandra FHIR Index brings together two of the most powerful products in the market [Apache Cassandra](http://cassandra.apache.org/) and [Apache Lucene](http://lucene.apache.org/). Apache Cassandra provides, among others high availability and scalability without impacting performance. On the other hand, Apache Lucene offers a robust full-featured text search engine.

The index operates on tables where FHIR Resources are stored as JSON content. At insert/update time, the index parses the JSON content with [HAPI-FHIR for Java](http://jamesagnew.github.io/hapi-fhir/) library, extracts the parameter values and indexes them into Lucene. During searching (using CQL SELECT query), Lucene is used to retrieve the resources that match the criteria. Query expressions are defined using the same syntax as [Lucene's Syntax](https://lucene.apache.org/core/5_2_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description). The index also supports custom keywords i.e. ORDER BY that extends the funcionalities of `QueryParser`.

Indexed data is stored locally to the node where the actual Cassandra data is stored. The documents in Lucene is tightly coupled with the live data; any create, update or deletion operation is atomically propagated to Lucene. This architecture presents the following benefits:

- No single point of failure for searches
- Linear scalability
- Automatic indexing of data added to Cassandra
- Automatic data replication
- Search support for CQL using expr syntax
- Reindexing of Cassandra tables using CQL
- Full-text search

## Motivation

The reason behind this implementation was to understand how FHIR can be supported in NoSQL databases. As there is already an implementation in MongoDB, I decided to explore Cassandra but after some initial testing I started seeing some limitations with queries. As you might know, Cassandra is really, really fast for writing, however in case of reading there are some use cases which are difficult to implement even using secondary indexes or materized views. To put this in others words, if you want to do wildcard,  range or just LIKE search, there is no built-in support for them. Please continue reading...

The recommended approach to deal with these issues is to duplicate and denormalize your data model, however in case of FHIR this is not feasible. FHIR defines search operations where there is a set of common parameters (around 10) plus many more specific parameters per Resource. For example, a Patient resource can be filtered by 25 different parameters: from identifier, name or email to some more complex attributes like address, deceased, gender, organization or phone. 

So, what can be done to deal with this situation? Is it possible to use Cassandra or not. And if yes, what is the best way to use Cassandra without compromising the benefits of such database? Well, I think I found some answers in these brillant products:

- [Stratio's Cassandra Lucene Index](https://github.com/Stratio/cassandra-lucene-index): The best library so far which uses Lucene as a search engine. The idea is simple but really powerful, they extended Cassandra secondary index to index any column using Lucene. As the secondary index is local to each Cassandra node, the queries are distributed across the cluster and each node has a Lucene index where the data is stored. When you execute a CQL query on the indexed column, Lucene is first queried to retrieve the partition keys of the SSTable rows. Then, the rows are returned from the table. The index is only used for filtering.
- [SASI Index](https://github.com/apache/cassandra/blob/trunk/doc/SASI.md): This is another implementation of Cassandra secondary index but this time, the indexes are created as tables in Cassandra. There is no dependencies with external products. SASI has been approved recently and is part of Cassandra 3.4 codebase. If you use 3.4, you can get the benefits of this index to filter by different criterias. At the moment the index does not support Not Equals and OR operators but I think is going to take more attention in the future when more organizations start moving to Cassandra. 
- [Stargate Core](http://stargate-core.readthedocs.org/index.html): Startgate is quite similar to Stratio's Cassandra Lucene Index, not sure who copy to whom, but the interesting different to me was that Stargate supports indexing and querying of JSON content. When you create a index on a JSON column, it will parse and index the complete JSON. This is quite useful when you have the same JSON schema and you would like to search for any atribute. However I can see some issues with this approach when you are dealing with big JSONs as it can slow the persistence as Cassandra get blocked until all the indexes have commited the changes. 

*** Please note Cassandra Secondary Index is not the silver bullet but it offers a convenient solution for this use case. 

## Features

Cassandra FHIR Index provides the following search options:

-  Single ``"?"`` and multiple ``"*"`` character wildcard searches within single expression
-  Regular Expression searches using forward slashes ``"/"``
-  Fuzzy searches using ``"~"`` at the end of the expression
-  Range searches between the lower and upper bound, i.e.: ``mod_date:[20020101 TO 20030101]``
-  Boolean searches operators ``(OR, AND, NOT, "+" and "-")``
-  Top-k queries (relevance scoring, sort by value)
-  Spark and Hadoop compatibility
-  Paging over filters

## Architecture

When a index is created using [CREATE CUSTOM INDEX](https://cassandra.apache.org/doc/cql3/CQL.html#createIndexStmt) statement, Cassandra will invoke the class's constructor defined with the `USING` option. At that moment, the Lucene components will be initialized and configured based on the index metadata. The implementation must implement few methods but the two more important are read (`Searcher`) and write (`Indexer`) operations. These implementations are the responsibles to interact with Lucene to search and store `Document`.


### Index Options

The index supports the following options:

Option | Description | Default
--- | --- | ---
refresh_seconds |  | 60 seconds |
ram_buffer_mb |  | 64 MB |
max_merge_mb |  | 5 MB |
max_cached_mb |  | 30 MB |
indexing_threads |  | 0 |
indexing_queues_size |  | 50 seconds |
excluded_data_centers |  | *empty* |
token_range_cache_size |  | 16 |
search_cache_size |  | 16 |
directory_path |  | lucene |
resource_type_column |  | *optional* |


## Quick start

The example walks through creating a table and index for a JSON column. Then shows how to performs queries on some inserted data.

The example assumes the `test` keyspace has been created and is in use.

```
cqlsh> CREATE KEYSPACE test WITH replication = {
   ... 'class': 'SimpleStrategy',
   ... 'replication_factor': '1'
   ... };
cqlsh> USE test;
```

Firstly, creates the table where you are going to store FHIR resources. Remember that one column will contain the JSON.

```
cqlsh:test> CREATE TABLE test.FHIR_RESOURCES (
    resource_id text,
    version int,
    resource_type text,
    state text,
    lastupdated timestamp,
    format text,
    author text,
    content text,
    PRIMARY KEY (resource_id, version, lastupdated)
);
```

### Creating an Index

Then create an index using CQL as follows:

```
cqlsh:test> CREATE CUSTOM INDEX idx_patient_name ON test.FHIR_RESOURCES (content)
     USING 'io.puntanegra.fhir.index.FhirIndex'
     WITH OPTIONS = {
        'refresh_seconds' : '5',
        'search': '{
            resources : {
			   Patient : ["name", "identifier", "family", "email", "active"],
			   Observation : ["code", "value-quantity"]
			}
        }'
     };
```



## Build and Installation

To build and install, first you will need the following tools:

-  Cassandra 3.0.4 or above
-  Java >= 1.8
-  Maven >= 3.0


TODO....