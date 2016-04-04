# cassandra-fhir-index
Cassandra's Secondary Index implementation for `FHIR® – Fast Healthcare Interoperability Resources <http://hl7.org/fhir/index.html>`. The index provides near real-time search capabilities and full-support (excluding chained and composite search parameters) for search operations defined in `FHIR search framework <http://hl7-fhir.github.io/search.html>`.  

This implementation brings together two of the most powerful products in the market `Apache Cassandra <http://cassandra.apache.org/>` and `Apache Lucene <http://lucene.apache.org/>`. On one hand, Apache Cassandra provides high availability and scalability without affecting response times. On the other hand, Apache Lucene gives you full-featured text search engine.

The index makes possible to deploy any FHIR-compliant server with Cassandra as datastore. For Health organizations, this means a whole new world of possibilities including real-time processing, analytics and big data.

The index operates on a table with a column where FHIR Resources are stored as JSON content. At insert/update time, the index parses the JSON content with `HAPI-FHIR for Java <http://jamesagnew.github.io/hapi-fhir/>` library, extracts the parameter values and index them into Lucene. During searching (using CQL SELECT query), Lucene is used to retrieve the resources that match the criteria. The query expression is defined using `Lucene's Syntax <https://lucene.apache.org/core/2_9_4/queryparsersyntax.html>` plus some custom keywords used mainly to extends the funcionalities.
