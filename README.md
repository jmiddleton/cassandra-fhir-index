# Cassandra FHIR Index
Cassandra `Index` implementation for [FHIR® – Fast Healthcare Interoperability Resources](http://hl7.org/fhir/index.html). The index provides near real-time search capabilities and full-support (excluding chained and composite search parameters) for search operations defined in [FHIR search framework](http://hl7-fhir.github.io/search.html).

This implementation brings together two of the most powerful products in the market [Apache Cassandra](http://cassandra.apache.org/) and [Apache Lucene](http://lucene.apache.org/). On one hand, Apache Cassandra provides high availability and scalability without affecting response times. On the other hand, Apache Lucene gives you full-featured text search engine.

The index makes possible to deploy any FHIR-compliant server with Cassandra as datastore. For Health organizations, this means a whole new world of possibilities including real-time processing, analytics and big data.

The index operates on a table with a column where FHIR Resources are stored as JSON content. At insert/update time, the index parses the JSON content with [HAPI-FHIR for Java](http://jamesagnew.github.io/hapi-fhir/) library, extracts the parameter values and index them into Lucene. During searching (using CQL SELECT query), Lucene is used to retrieve the resources that match the criteria. The query expression is defined using [Lucene's Syntax](https://lucene.apache.org/core/5_2_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description) plus some custom keywords used mainly to extends the funcionalities.

## Motivation

The real motivation behind this implementation was to support FHIR especification on NoSQL databases. I understand there is already an implementation in MongoDB, however when we talk about NoSQL, usually we associate the term with Apache Cassandra so when I was looking how to implement FHIR on Cassandra, I faced the issue with queries. As you might know Cassandra is really fast and scalable for writing, however if you want to filter your data the only way is to use the partition key or clustering columns. There is no much built-in capabilities (minimal filtering options with secondary indexes or materized views) to filter by a column that is not part of the primary key.

The recommended approach to deal with this is to duplicate and denormalize your data model, however in case of FHIR this is not feasible. FHIR Search Framework defines search operations as a set of common parameters plus many more Resource specific parameters. For instance, a Patient can be filtered by around 25 different parameters, from identifier, name, email to some more complex attributes like address, deceased, gender, organization or phone. 

So, how can we do it? Is it possible or not and if yes, what is the best way to use Cassandra without compromising performance or any of the benefits of the database. Well, I think I found the answers or at least part of them in three brillant products:

- [Stratio's Cassandra Lucene Index](https://github.com/Stratio/cassandra-lucene-index): The best library so far which uses Lucene as a search engine. The idea is simple but really powerful, they extended Cassandra secondary index to index any column using Lucene. As the secondary index is local to each Cassandra node, the queries are distributed across the cluster and each node has a Lucene index where the data is stored. When you execute a CQL query on the indexed column, Lucene is first queried to retrieve the partition keys of the SSTable rows. Then, the rows are returned from the table. The index is only used for filtering.
- [SASI Index](https://github.com/apache/cassandra/blob/trunk/doc/SASI.md): This is another implementation of Cassandra secondary index but this time, the indexes are created as tables in Cassandra. There is no dependencies with external products. SASI has been approved recently and is part of Cassandra 3.4 codebase. If you use 3.4, you can get the benefits of this index to filter by different criterias. At the moment the index does not support Not Equals and OR operators but I think is going to take more attention in the future when more organizations start moving to Cassandra. 
- [Stargate Core](http://stargate-core.readthedocs.org/index.html): Startgate is quite similar to Stratio's Cassandra Lucene Index, not sure who copy to whom, but the interesting different to me was that Stargate supports indexing and querying of JSON content. When you create a index on a JSON column, it will parse and index the complete JSON. This is quite useful when you have the same JSON schema and you would like to search for any atribute. However I can see some issues with this approach when you are dealing with big JSONs as it can slow the persistence as Cassandra get blocked until all the indexes have commited the changes. 


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

Along with this, Cassandra provides a number of key features as Linear Scalability, Fault-Tolerance, High Availability, Multi Datacenter Replication, Low Latency, Flexible Data Model, Data Compression and so on. 

## Architecture


## Quick start

The example walks through creating a table and index for a JSON column. Then shows how to performs queries on some inserted data.

The examples below assume the `test` keyspace has been created and is in use.

```
cqlsh> CREATE KEYSPACE test WITH replication = {
   ... 'class': 'SimpleStrategy',
   ... 'replication_factor': '1'
   ... };
cqlsh> USE test;
```


## Requirements

-  Cassandra 3.0.4 or above
-  Java >= 1.8
-  Maven >= 3.0


TODO....