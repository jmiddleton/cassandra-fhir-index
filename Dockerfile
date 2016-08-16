# vim:set ft=dockerfile:
FROM cassandra:3.7

ARG VERSION

COPY fhir-index-plugin/target/fhir-index-plugin-${VERSION}.jar /usr/share/cassandra/lib/
