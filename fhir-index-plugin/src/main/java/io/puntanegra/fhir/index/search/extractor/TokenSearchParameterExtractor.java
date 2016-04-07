package io.puntanegra.fhir.index.search.extractor;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.dstu3.model.CodeableConcept;
import org.hl7.fhir.dstu3.model.Coding;
import org.hl7.fhir.dstu3.model.Conformance.ConformanceRestSecurityComponent;
import org.hl7.fhir.dstu3.model.ContactPoint;
import org.hl7.fhir.dstu3.model.Enumeration;
import org.hl7.fhir.dstu3.model.Identifier;
import org.hl7.fhir.dstu3.model.Location.LocationPositionComponent;
import org.hl7.fhir.dstu3.model.Patient.PatientCommunicationComponent;
import org.hl7.fhir.dstu3.model.ValueSet;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamToken;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Extracts information from a parameter of type Token.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class TokenSearchParameterExtractor extends AbstractSearchParameterExtractor {

	public TokenSearchParameterExtractor(FhirContext ctx) {
		super(ctx);
	}

	@Override
	public Set<AbstractSearchParam> extractValues(IBaseResource instance, RuntimeSearchParam searchParam) {

		Set<AbstractSearchParam> values = new HashSet<AbstractSearchParam>();

		String path = searchParam.getPath();
		String resourceName = searchParam.getName();
		String paramType = getParamType(searchParam);

		boolean multiType = false;
		if (path.endsWith("[x]")) {
			multiType = true;
		}

		List<String> systems = new ArrayList<String>();
		List<String> codes = new ArrayList<String>();

		String needContactPointSystem = null;
		if (path.endsWith(".where(system='phone')")) {
			path = path.substring(0, path.length() - ".where(system='phone')".length());
			needContactPointSystem = "phone";
		}
		if (path.endsWith(".where(system='email')")) {
			path = path.substring(0, path.length() - ".where(system='email')".length());
			needContactPointSystem = "email";
		}

		for (Object obj : extractValues(path, instance)) {

			// Patient:language
			if (obj instanceof PatientCommunicationComponent) {
				PatientCommunicationComponent nextValue = (PatientCommunicationComponent) obj;
				obj = nextValue.getLanguage();
			}

			if (obj instanceof Identifier) {
				Identifier identifier = (Identifier) obj;
				if (identifier.isEmpty()) {
					continue;
				}
				String system = StringUtils.defaultIfBlank(identifier.getSystemElement().getValueAsString(), null);
				String value = identifier.getValueElement().getValue();
				if (isNotBlank(value)) {
					systems.add(system);
					codes.add(value);
				}

				if (isNotBlank(identifier.getType().getText())) {
					values.add(addStringParam(searchParam, identifier.getType().getText()));
				}

			} else if (obj instanceof ContactPoint) {
				ContactPoint nextValue = (ContactPoint) obj;
				if (nextValue.isEmpty()) {
					continue;
				}
				if (isNotBlank(needContactPointSystem)) {
					if (!needContactPointSystem.equals(nextValue.getSystemElement().getValueAsString())) {
						continue;
					}
				}
				systems.add(nextValue.getSystemElement().getValueAsString());
				codes.add(nextValue.getValueElement().getValue());
			} else if (obj instanceof Enumeration<?>) {
				Enumeration<?> en = (Enumeration<?>) obj;
				String system = extractSystem(en);
				String code = en.getValueAsString();
				if (isNotBlank(code)) {
					systems.add(system);
					codes.add(code);
				}
			} else if (obj instanceof IPrimitiveType<?>) {
				IPrimitiveType<?> nextValue = (IPrimitiveType<?>) obj;
				if (nextValue.isEmpty()) {
					continue;
				}
				if ("ValueSet.codeSystem.concept.code".equals(path)) {
					String useSystem = null;
					if (instance instanceof ValueSet) {
						ValueSet vs = (ValueSet) instance;
						useSystem = vs.getCodeSystem().getSystem();
					}
					systems.add(useSystem);
				} else {
					systems.add(null);
				}
				codes.add(nextValue.getValueAsString());
			} else if (obj instanceof Coding) {
				Coding coding = (Coding) obj;
				extractTokensFromCoding(systems, codes, searchParam, coding, values);
			} else if (obj instanceof CodeableConcept) {
				CodeableConcept codeable = (CodeableConcept) obj;
				if (!codeable.getTextElement().isEmpty()) {
					values.add(addStringParam(searchParam, codeable.getTextElement().getValue()));
				}

				for (Coding coding : codeable.getCoding()) {
					extractTokensFromCoding(systems, codes, searchParam, coding, values);
				}
			} else if (obj instanceof ConformanceRestSecurityComponent) {
				// Conformance.security search param points to something kind of
				// useless right now - This should probably
				// be fixed.
				ConformanceRestSecurityComponent sec = (ConformanceRestSecurityComponent) obj;
				for (CodeableConcept nextCC : sec.getService()) {
					if (!nextCC.getTextElement().isEmpty()) {
						values.add(addStringParam(searchParam, nextCC.getTextElement().getValue()));
					}
				}
			} else if (obj instanceof LocationPositionComponent) {
				logger.warn("Position search not currently supported, not indexing location");
				continue;
			} else {
				if (!multiType) {
					throw new SearchParameterException(
							"Search param " + resourceName + " is of unexpected datatype: " + obj.getClass());
				} else {
					continue;
				}
			}
		}

		assert systems.size() == codes.size() : "Systems contains " + systems + ", codes contains: " + codes;

		for (int i = 0; i < systems.size(); i++) {
			String system = systems.get(i);
			String code = codes.get(i);
			if (isBlank(system) && isBlank(code)) {
				continue;
			}

			if (system != null && system.length() > MAX_LENGTH) {
				system = system.substring(0, MAX_LENGTH);
			}

			if (code != null && code.length() > MAX_LENGTH) {
				code = code.substring(0, MAX_LENGTH);
			}

			SearchParamToken token = new SearchParamToken(resourceName, path, SearchParamTypes.valueOf(paramType),
					system, code);
			values.add(token);

		}

		return values;
	}

	private void extractTokensFromCoding(List<String> systems, List<String> codes, RuntimeSearchParam searchParam,
			Coding coding, Set<AbstractSearchParam> values) {
		if (coding != null && !coding.isEmpty()) {

			String nextSystem = coding.getSystemElement().getValueAsString();
			String nextCode = coding.getCodeElement().getValue();
			if (isNotBlank(nextSystem) || isNotBlank(nextCode)) {
				systems.add(nextSystem);
				codes.add(nextCode);
			}
		}
	}

}
