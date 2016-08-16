package io.puntanegra.fhir.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileReader;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.ContactPoint;
import org.hl7.fhir.dstu3.model.ContactPoint.ContactPointSystem;
import org.hl7.fhir.dstu3.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.dstu3.model.HumanName;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.Patient;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.junit.Test;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import io.puntanegra.fhir.index.search.SearchParamExtractorHelper;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamDates;
import io.puntanegra.fhir.index.search.datatypes.SearchParamToken;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

public class SearchParamExtractorTest {
	private FhirVersionEnum fhirVersion = FhirVersionEnum.DSTU3;
	private FhirContext ctx = new FhirContext(fhirVersion);
	private SearchParamExtractorHelper helper = new SearchParamExtractorHelper(FhirVersionEnum.DSTU3);

	@Test
	public void testExtractParam() throws Exception {
		FileReader fileReader = new FileReader(
				new File(this.getClass().getClassLoader().getResource("fhir/observation_example001.json").getPath()));
		IBaseResource resource = ctx.newJsonParser().parseResource(fileReader);

		Set<String> parameters = new HashSet<String>();
		parameters.add("code");
		parameters.add("value-quantity");
		parameters.add("date");

		Set<AbstractSearchParam> values = helper.extractParametersValues(resource, parameters);

		assertNotNull(values);
		assertFalse(values.isEmpty());

		for (AbstractSearchParam entry : values) {
			String ename = entry.getName();
			SearchParamTypes type = entry.getType();

			if (SearchParamTypes.TOKEN == type) {
				SearchParamToken token = (SearchParamToken) entry;
				if (ename.equals("code") && "8480-6".equals(token.getCode())) {
					assertEquals("Observation.code | Observation.component.code", token.getPath());
				}
			}if (SearchParamTypes.DATE == type) {
				SearchParamDates dates = (SearchParamDates) entry;
				if (ename.equals("date")) {
					Calendar date= Calendar.getInstance();
					date.setTime(dates.getValue());
					assertEquals(4, date.get(Calendar.MONTH));
				}
			}
		}
	}

	@Test
	public void testLoadPatient() throws Exception {
		FileReader fileReader = new FileReader(
				new File(this.getClass().getClassLoader().getResource("fhir/patient_f001.json").getPath()));
		IBaseResource resource = ctx.newJsonParser().parseResource(fileReader);

		Set<String> parameters = new HashSet<String>();
		parameters.add("name");
		parameters.add("email");

		Set<AbstractSearchParam> values = helper.extractParametersValues(resource, parameters);

		assertNotNull(values);
		assertFalse(values.isEmpty());

		for (AbstractSearchParam entry : values) {
			String ename = entry.getName();
			SearchParamTypes type = entry.getType();

			if (SearchParamTypes.STRING == type) {
				if (ename.equals("name")) {
					assertEquals("van de Heuvel Pieter", entry.getValue());
				}
			}

			if (SearchParamTypes.TOKEN == type) {
				SearchParamToken token = (SearchParamToken) entry;
				if (ename.equals("email")) {
					assertEquals("email", token.getSystem());

					List<ContactPoint> contacts = ((Patient) resource).getTelecom();
					for (ContactPoint contactPoint : contacts) {
						if (ContactPointSystem.EMAIL == contactPoint.getSystem()) {
							assertEquals(contactPoint.getValue(), token.getCode());
						}
					}
				}
			}
		}
	}

	@Test
	public void testSearchParamValues() {

		Patient patient = new Patient();
		patient.setId("253345");

		Calendar c = Calendar.getInstance();
		c.set(1998, 3, 3);
		patient.setBirthDate(c.getTime());

		CodeableConcept language = patient.addCommunication().getLanguage();
		language.setText("Nederlands");

		Coding coding = language.addCoding();
		coding.setSystem("urn:ietf:bcp:47");
		coding.setCode("nl");
		coding.setDisplay("Dutch");
		patient.addCommunication().setLanguage(language);

		HumanName name = patient.addName();
		name.addFamily("Smith");
		name.addGiven("Rob").addGiven("Jon");

		Identifier id = new Identifier();
		id.setValue("253345");
		id.setSystem("urn:mrns");
		patient.addIdentifier(id);
		// patient.getManagingOrganization().setReference("Organization/124362");

		patient.setGender(AdministrativeGender.MALE);
		ContactPoint cp = new ContactPoint();
		cp.setSystem(ContactPointSystem.EMAIL);
		cp.setValue("rob@gmail.com");
		patient.addTelecom(cp);

		// Coded types can naturally be set using plain strings
		Coding statusCoding = patient.getMaritalStatus().addCoding();
		statusCoding.setSystem("http://hl7.org/fhir/v3/MaritalStatus");
		statusCoding.setCode("M");
		statusCoding.setDisplay("Married");

		Set<String> parameters = new HashSet<String>();
		parameters.add("family");
		parameters.add("email");
		parameters.add("identifier");
		parameters.add("birthdate");
		parameters.add("name");
		parameters.add("language");

		Set<AbstractSearchParam> values = helper.extractParametersValues(patient, parameters);

		assertNotNull(values);
		assertFalse(values.isEmpty());

		for (AbstractSearchParam entry : values) {
			String ename = entry.getName();
			SearchParamTypes type = entry.getType();

			if (SearchParamTypes.STRING == type) {
				if (ename.equals("family")) {
					assertEquals("Smith", entry.getValue());
				}

				if (ename.equals("identifier")) {
					assertEquals("253345", entry.getValue());
				}
			}

			if (SearchParamTypes.TOKEN == type) {
				SearchParamToken token = (SearchParamToken) entry;
				if (ename.equals("email")) {
					assertEquals("email", token.getSystem());
					assertEquals("rob@gmail.com", token.getCode());
				}

				if (ename.equals("identifier")) {
					assertEquals("urn:mrns", token.getSystem());
				}

				if (ename.equals("language")) {
					assertEquals("nl", token.getCode());
					assertEquals("urn:ietf:bcp:47", token.getSystem());
				}
			}

			if (SearchParamTypes.DATE == type) {
				SearchParamDates dates = (SearchParamDates) entry;

				if (ename.equals("birthdate")) {
					assertEquals(c.getTime().getTime(), dates.getValue().getTime());
				}
			}
		}
	}

}
