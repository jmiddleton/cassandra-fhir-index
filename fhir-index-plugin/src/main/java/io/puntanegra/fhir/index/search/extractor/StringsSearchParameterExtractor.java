package io.puntanegra.fhir.index.search.extractor;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.dstu3.model.Address;
import org.hl7.fhir.dstu3.model.ContactPoint;
import org.hl7.fhir.dstu3.model.DateType;
import org.hl7.fhir.dstu3.model.HumanName;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.instance.model.api.IPrimitiveType;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamDates;
import io.puntanegra.fhir.index.search.datatypes.SearchParamString;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Extracts information from string attribute types.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class StringsSearchParameterExtractor extends AbstractSearchParameterExtractor {

	public StringsSearchParameterExtractor(FhirContext ctx) {
		super(ctx);
	}

	@Override
	public Set<AbstractSearchParam> extractValues(IBaseResource instance, RuntimeSearchParam searchParam) {

		Set<AbstractSearchParam> values = new HashSet<AbstractSearchParam>();

		String path = searchParam.getPath();
		String resourceName = searchParam.getName();
		String paramType = getParamType(searchParam);

		for (Object nextObject : extractValues(path, instance)) {
			if (nextObject == null || ((IBase) nextObject).isEmpty()) {
				continue;
			}

			boolean multiType = false;
			if (path.endsWith("[x]")) {
				multiType = true;
			}

			// TODO: check if it is better to return the original value instead
			// of the String representation
			if (nextObject instanceof IPrimitiveType<?>) {
				IPrimitiveType<?> primitive = (IPrimitiveType<?>) nextObject;
				String searchTerm = primitive.getValueAsString();

				AbstractSearchParam def = null;
				if (primitive instanceof DateType) {
					def = new SearchParamDates(resourceName, path, SearchParamTypes.valueOf(paramType),
							((DateType) primitive).getValue(), null);
				} else {
					def = new SearchParamString(resourceName, path, SearchParamTypes.valueOf(paramType), searchTerm);
				}
				values.add(def);

			} else {
				if (nextObject instanceof HumanName) {
					ArrayList<String> allNames = new ArrayList<String>();
					HumanName nextHumanName = (HumanName) nextObject;
					allNames.add(nextHumanName.getFamilyAsSingleString());
					allNames.add(nextHumanName.getGivenAsSingleString());

					SearchParamString def = new SearchParamString(resourceName, path,
							SearchParamTypes.valueOf(paramType), StringUtils.join(allNames, ' '));
					values.add(def);

				} else if (nextObject instanceof Address) {
					ArrayList<String> allNames = new ArrayList<String>();
					Address nextAddress = (Address) nextObject;
					// TODO: allNames.addAll(nextAddress.getLine());
					allNames.add(nextAddress.getCityElement().asStringValue());
					allNames.add(nextAddress.getStateElement().asStringValue());
					allNames.add(nextAddress.getCountryElement().asStringValue());
					allNames.add(nextAddress.getPostalCodeElement().asStringValue());

					SearchParamString def = new SearchParamString(resourceName, path,
							SearchParamTypes.valueOf(paramType), StringUtils.join(allNames, ' '));
					values.add(def);

				} else if (nextObject instanceof ContactPoint) {
					ContactPoint contact = (ContactPoint) nextObject;
					if (contact.getValueElement().isEmpty() == false) {

						SearchParamString def = new SearchParamString(resourceName, path,
								SearchParamTypes.valueOf(paramType), contact.getValue());
						values.add(def);
					}
				} else {
					if (!multiType) {
						//throw new SearchParameterException("Search param " + resourceName
							//	+ " is of unexpected datatype: " + nextObject.getClass());
					}
				}
			}
		}
		return values;
	}

}
