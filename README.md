# Cassandra FHIR Index

Cassandra `Index` implementation for [FHIR® – Fast Healthcare Interoperability Resources](http://hl7.org/fhir/index.html). The index provides near real-time search capabilities and full-support (excluding chained and composite search parameters) for search operations defined in [FHIR Search Framework](http://hl7-fhir.github.io/search.html).

Cassandra FHIR Index brings together two of the most powerful products in the market [Apache Cassandra](http://cassandra.apache.org/) and [Apache Lucene](http://lucene.apache.org/). Apache Cassandra provides, among others high availability and scalability without impacting performance. On the other hand, Apache Lucene offers a robust full-featured text search engine.

The index operates on tables where FHIR Resources are stored as JSON content. At insert/update time, the index parses the JSON content with [HAPI-FHIR for Java](http://jamesagnew.github.io/hapi-fhir/) library, extracts the parameter values and indexes them into Lucene. During searching (using CQL SELECT query), Lucene is used to retrieve the resources that match the criteria. Query expressions are defined using the same syntax as [Lucene's Syntax](https://lucene.apache.org/core/5_2_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description). The index also supports custom keywords i.e. ORDER BY that extends the funcionalities of `QueryParser`.

Indexed data is stored locally to the node where the actual Cassandra data is stored. The documents in Lucene is tightly coupled with the live data; any create, update or deletion operation is atomically propagated to Lucene. This architecture presents the following benefits:

-  No single point of failure for searches
-  Linear scalability
-  Automatic indexing of data added to Cassandra
-  Automatic data replication
-  Search support for CQL using expr syntax
-  Reindexing of Cassandra tables using CQL
-  Full-text search
-  Data locality
-  Transparent integration with Apache Spark

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
-  Token Range support

## Architecture

The high level architecture consists of Cassandra nodes each of them with a local Lucene index. When a table is updated, the Lucene document is automatically updated. This uses data locality so each node will indexes the data that is stored locally. The update and indexing is done atomically so there is a performance impact when writing. 

On searching, the implementation requires few extra steps. When a user executes a `CQL SELECT` statement, the request is first processed by a random coordinator. Then, the coordinator sends the query to each node in the cluster. Each node searches locally in the Lucene index and returns its results. Once all the results are back in the coordinator, it merge the results and returns only the top n matches.

When a index is created using [CREATE CUSTOM INDEX](https://cassandra.apache.org/doc/cql3/CQL.html#createIndexStmt) statement, Cassandra instanciates the class defined by the `USING` option. At this time, the Lucene components will be configured based on the index metadata and the index will be created in the Cassandra node. Along with the Index, there are two important interfaces to implement: 

- `Searcher`: performs queries on the Lucene index based on the CQL condition. 
- `Indexer`: event listener which processes events emitted during partition updates (create or delete). This implementation upserts Lucene documents in the local index.

Each Lucene document is composed of multiple key-value pairs where each key represent a FHIR Resource parameter. Along with that, the document also stores the partition-key of the real data, the partitioner's token, the clustering columns and the resource type.

### Index Options

The index supports the following options:

Option | Description | Default
--- | --- | ---
refresh_seconds | Amount of seconds to wait until the next index refresh. | 60 seconds |
ram_buffer_mb |  | 64 MB |
max_merge_mb |  | 5 MB |
max_cached_mb |  | 30 MB |
indexing_threads |  | 0 |
indexing_queues_size |  | 50 seconds |
excluded_data_centers |  | *empty* |
token_range_cache_size |  | 16 |
search_cache_size |  | 16 |
directory_path | Relative path of the directory where Lucene indexes will be stored. This path is relative to $CASSANDRA/data folder | lucene |
resource_type_column | Column name of the column that stores the FHIR Resource Type. This is used when you only want to index a specific resource, i.e.: Observation | *optional* |

#### Search Option
Defines which FHIR Resources to index. The format is as follows:

```
resources : {
   [FHIR Resource Type] : ["parameter1", "parameter2", "parameter3", "parameter4"]
}

Example:

resources : {
    Patient : ["name", "identifier", "family", "email", "active"],
    Observation : ["code", "value-quantity", "performer", "subject", "status", "category"],
    AllergyIntolerance : ["date", "patient", "status", "type"],
}
```

If `resources` is not defined, the index will index all the resources found by the HAPI-FHIR library.

During initialization the index will validate if the configuration is correct or not. In case of errors, the creation of the index will fail and an error message will be displayed.

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
cqlsh:test> CREATE TABLE FHIR_RESOURCES (
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
cqlsh:test> CREATE CUSTOM INDEX idx_fhir_resources ON FHIR_RESOURCES (content)
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

### Inserting Test Data

Now that the index is created, we can insert some test data. To load test data into Cassandra execute the following command.

```

//TODO: create a class to load data in Cassandra

```


## Build and Installation

To build and install, first you will need the following tools:

-  Cassandra 3.0.4 or above
-  Java >= 1.8
-  Maven >= 3.0


TODO....