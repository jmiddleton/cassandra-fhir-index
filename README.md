# Cassandra FHIR Index
Cassandra `Index` implementation for `FHIR® – Fast Healthcare Interoperability Resources <http://hl7.org/fhir/index.html>`. The index provides near real-time search capabilities and full-support (excluding chained and composite search parameters) for search operations defined in `FHIR search framework <http://hl7-fhir.github.io/search.html>`.  

This implementation brings together two of the most powerful products in the market `Apache Cassandra <http://cassandra.apache.org/>` and `Apache Lucene <http://lucene.apache.org/>`. On one hand, Apache Cassandra provides high availability and scalability without affecting response times. On the other hand, Apache Lucene gives you full-featured text search engine.

The index makes possible to deploy any FHIR-compliant server with Cassandra as datastore. For Health organizations, this means a whole new world of possibilities including real-time processing, analytics and big data.

The index operates on a table with a column where FHIR Resources are stored as JSON content. At insert/update time, the index parses the JSON content with `HAPI-FHIR for Java <http://jamesagnew.github.io/hapi-fhir/>` library, extracts the parameter values and index them into Lucene. During searching (using CQL SELECT query), Lucene is used to retrieve the resources that match the criteria. The query expression is defined using `Lucene's Syntax <https://lucene.apache.org/core/5_2_1/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description>` plus some custom keywords used mainly to extends the funcionalities.

Features
========

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
