package io.puntanegra.fhir.index;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class FhirIndexServiceTest extends InitCassandraKeyspace {
	private Session session;

	//@formatter:off
	private static final String patientJson= "{"+
		    "\"resourceType\":\"Patient\","+
		    "\"id\":\"34a4e1c6-57c2-4217-8478-9c3f10b7aaaa\","+
		    "\"name\":["+
		    "    {"+
		    "        \"family\":["+
		    "            \"Peroni\""+
		    "        ],"+
		    "        \"given\":["+
		    "            \"Marcelo\""+
		    "        ]"+
		    "    }"+
		    "],"+
		    "\"active\": true," +
		    "\"telecom\": ["+
		    "  {"+
		    "    \"system\": \"phone\","+
		    "    \"value\": \"0648352638\","+
		    "    \"use\": \"mobile\""+
		    "  },"+
		    "  {"+
		    "    \"system\": \"email\","+
		    "    \"value\": \"juan.perez@gmail.com\","+
		    "    \"use\": \"home\""+
		    "  }"+
		    "],"+
		    "\"gender\":\"male\","+
		    "\"birthDate\":\"1977-04-16\""+
		    "}";
	//@formatter:on

	@Before
	public void init() throws Exception {
		initCassandraFS();
		
		Cluster cluster = Cluster.builder().addContactPoints("localhost").withProtocolVersion(ProtocolVersion.V4)
				.build();
		session = cluster.connect();

		session.execute("USE test;");
	}

	@Test
	public void test() throws Exception {

		session.execute(
				"INSERT INTO test.FHIR_RESOURCES (resource_id, version, resource_type, state, lastupdated, format, author, content)"
						+ " VALUES ('pat556eb333', 1, 'Patient', 'active', 1442959315019, 'json', 'dr who'," + "'"
						+ patientJson + "')");

		Thread.sleep(1000);
		ResultSet r = session.execute(
				"SELECT * FROM test.FHIR_RESOURCES" + " WHERE expr(idx_fhir_resources, 'active:true')" + " LIMIT 100;");
		//@formatter:on

		List<Row> l = r.all();
		assertEquals(1, l.size());

		for (Row row : l) {
			System.out.println(">>>>>>>>>>>>>>>>" + row.toString());
			// assertEquals("556ebd54", row.getString("resource_id"));
		}

	}

	@After
	public void tearDown() {
		// session.close();
	}

}
