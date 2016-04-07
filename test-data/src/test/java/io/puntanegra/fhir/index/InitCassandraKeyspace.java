package io.puntanegra.fhir.index;

import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.commons.io.FileUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

public class InitCassandraKeyspace {
	private static final String RESOURCE_COMMITLOG = "/tmp/commitlog";
	private static final String RESOURCE_DATA = "/tmp/data";
	protected Session session;

	public void initCassandraFS() throws Exception {
		FileUtils.forceDeleteOnExit(FileUtils.getFile(RESOURCE_DATA));
		FileUtils.forceDeleteOnExit(FileUtils.getFile(RESOURCE_COMMITLOG));

		System.setProperty("CASSANDRA_HOME", "~/Apps/apache-cassandra-3.0.4");
		System.setProperty("cassandra.config", "cassandra.yaml");
		System.setProperty("storage-config", RESOURCE_DATA);

		EmbeddedCassandraService cassandraService = new EmbeddedCassandraService();

		cassandraService.start();

		Cluster cluster = Cluster.builder().addContactPoints("localhost").withProtocolVersion(ProtocolVersion.V4)
				.build();
		session = cluster.connect();

		//@formatter:off
		//session.execute("DROP KEYSPACE test");
		session.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};");
		session.execute("USE test;");
		
		//session.execute("DROP TABLE test.FHIR_RESOURCES;");
		session.execute("CREATE TABLE IF NOT EXISTS test.FHIR_RESOURCES ("+
		    "resource_id text,"+
		    "version int,"+
		    "resource_type text,"+
		    "state text,"+
		    "lastupdated timestamp,"+
		    "format text,"+
		    "author text,"+
		    "content text,"+
		    "PRIMARY KEY (resource_id, version, lastupdated));");
		
		session.execute("CREATE CUSTOM INDEX IF NOT EXISTS idx_fhir_resources ON test.FHIR_RESOURCES (content)"+
				"USING 'io.puntanegra.fhir.index.FhirIndex'"+
				"WITH OPTIONS = {"+
				"    'refresh_seconds' : '5',"+
				"    'search' : '{"+
				"        resources : {"+
				"            Patient : [\"family\", \"email\", \"active\"],"+
				"            Observation : [\"code\", \"value-quantity\"]"+
				"        }"+
				"    }'"+
				"};");
		
//		session.execute("CREATE CUSTOM INDEX IF NOT EXISTS idx_patient ON test.FHIR_RESOURCES (content)"+
//			"USING 'io.puntanegra.fhir.index.FhirIndex'"+
//			"WITH OPTIONS = {"+
//			"    'refresh_seconds' : '5',"+
//			"    'resource_type' : 'Patient',"+
//			"    'resource_type_column' : 'resource_type',"+
//			"    'search' : '{"+
//			"        parameters : [" +
//            "            \"active\", " +
//            "            \"family\", " +
//            "            \"email\", " +
//            "            \"identifier\"" +
//            "         ]" +
//			"    }'"+
//			"};");

		//@formatter:off
//		session.execute("CREATE CUSTOM INDEX IF NOT EXISTS idx_observation ON test.FHIR_RESOURCES (content)"+
//				"USING 'io.puntanegra.fhir.index.FhirIndex'"+
//				"WITH OPTIONS = {"+
//				"    'refresh_seconds' : '5',"+
//				"    'resource_type' : 'Observation',"+
//		        "    'resource_type_column' : 'resource_type',"+
//				"    'search' : '{"+
//				"        parameters : [" +
//				"            \"code\", " +
//				"            \"value-quantity\"" +
//				"         ]" +
//				"    }'"+
//				"};");
		//@formatter:on
	}

}
