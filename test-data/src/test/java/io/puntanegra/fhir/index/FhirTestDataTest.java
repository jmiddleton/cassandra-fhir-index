package io.puntanegra.fhir.index;

import java.io.File;
import java.io.FileReader;
import java.util.Calendar;

import org.hl7.fhir.dstu3.model.ContactPoint;
import org.hl7.fhir.dstu3.model.ContactPoint.ContactPointSystem;
import org.hl7.fhir.dstu3.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.dstu3.model.HumanName;
import org.hl7.fhir.dstu3.model.Observation;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.parser.IParser;

public class FhirTestDataTest {
	private FhirVersionEnum fhirVersion = FhirVersionEnum.DSTU3;
	private FhirContext ctx = new FhirContext(fhirVersion);

	private Session session;

	@Before
	public void init() throws Exception {

		String cassandraServer = System.getProperty("CassandraNode");

		if (cassandraServer == null || cassandraServer.length() == 0) {
			cassandraServer = "localhost";
		}

		Cluster cluster = Cluster.builder().addContactPoints(cassandraServer).withProtocolVersion(ProtocolVersion.V4)
				.build();
		session = cluster.connect();
		session.execute("USE test;");
	}

	@Test
	public void loadTestData() throws Exception {
		loadObservationData();
		loadPatientData();
	}

	public void loadObservationData() throws Exception {
		IParser parser = ctx.newJsonParser();

		FileReader fileReader = new FileReader(
				new File(this.getClass().getClassLoader().getResource("fhir/observation_example001.json").getPath()));
		IBaseResource resource = parser.parseResource(fileReader);

		for (int i = 0; i < 100; i++) {

			resource.getIdElement().setValue("obs_" + i);
			((Observation) resource).getIdentifier().get(0).setValue("urn:uuid:187e0c12-8dd2-67e2-99b2-bf273c1111" + i);

			String json = parser.encodeResourceToString(resource);

			long timestamp = Calendar.getInstance().getTimeInMillis();
			session.execute(
					"INSERT INTO test.FHIR_RESOURCES (resource_id, version, resource_type, state, lastupdated, format, author, content)"
							+ " VALUES ('" + resource.getIdElement().getValue() + "', 1, '"
							+ resource.getClass().getSimpleName() + "', 'active', " + timestamp + ", 'json', 'dr who',"
							+ "'" + json + "')");

			System.out.println(resource.getClass().getSimpleName() + ": " + resource.getIdElement().getValue());
		}
	}

	public void loadPatientData() throws Exception {
		IParser parser = ctx.newJsonParser();
		NameGenerator nameGenerator = new NameGenerator();

		FileReader fileReader = new FileReader(
				new File(this.getClass().getClassLoader().getResource("fhir/patient_f001.json").getPath()));
		IBaseResource resource = parser.parseResource(fileReader);

		for (int i = 0; i < 100; i++) {
			String family = nameGenerator.getName();
			String given1 = nameGenerator.getName();
			String email = given1.toLowerCase() + "." + family.toLowerCase() + "@gmail.com";

			resource.getIdElement().setValue("pat_" + i);
			Patient patient = (Patient) resource;
			patient.getIdentifier().get(0).setValue(resource.getIdElement().getValue());

			HumanName name = patient.getName().get(0);
			name.getFamily().clear();
			name.getGiven().clear();
			name.addFamily(family);
			name.addGiven(given1).addGiven(nameGenerator.getName());

			patient.setGender(i % 2 == 0 ? AdministrativeGender.MALE : AdministrativeGender.FEMALE);

			patient.getTelecom().clear();
			ContactPoint cp = new ContactPoint();
			cp.setSystem(ContactPointSystem.EMAIL);
			cp.setValue(email);
			patient.addTelecom(cp);

			String json = parser.encodeResourceToString(resource);

			long timestamp = Calendar.getInstance().getTimeInMillis();
			session.execute(
					"INSERT INTO test.FHIR_RESOURCES (resource_id, version, resource_type, state, lastupdated, format, author, content)"
							+ " VALUES ('" + resource.getIdElement().getValue() + "', 1, '"
							+ resource.getClass().getSimpleName() + "', 'active', " + timestamp + ", 'json', 'dr who',"
							+ "'" + json + "')");
			System.out.println(resource.getClass().getSimpleName() + ": " + resource.getIdElement().getValue()
					+ ", family:" + family + ", email:" + email);
		}
	}

}
