package io.puntanegra.fhir.index.search.extractor;

import java.util.HashSet;
import java.util.Set;

import org.hl7.fhir.dstu3.model.BaseDateTimeType;
import org.hl7.fhir.dstu3.model.Period;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamDates;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Extracts information from dates attribute types.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class DatesSearchParameterExtractor extends AbstractSearchParameterExtractor {

	public DatesSearchParameterExtractor(FhirContext ctx) {
		super(ctx);
	}

	@Override
	public Set<AbstractSearchParam> extractValues(IBaseResource instance, RuntimeSearchParam searchParam) {

		Set<AbstractSearchParam> values = new HashSet<AbstractSearchParam>();

		String path = searchParam.getPath();
		String resourceName = searchParam.getName();
		String paramType = getParamType(searchParam);

		for (Object obj : extractValues(path, instance)) {
			if (obj == null || ((IBase) obj).isEmpty()) {
				continue;
			}

			boolean multiType = false;
			if (path.endsWith("[x]")) {
				multiType = true;
			}

			if (obj instanceof BaseDateTimeType) {
				BaseDateTimeType datetime = (BaseDateTimeType) obj;
				if (datetime.isEmpty()) {
					continue;
				}
				SearchParamDates defq = new SearchParamDates(resourceName, path, SearchParamTypes.valueOf(paramType),
						datetime.getValue(), null);
				values.add(defq);
			} else if (obj instanceof Period) {
				Period period = (Period) obj;
				if (period.isEmpty()) {
					continue;
				}
				SearchParamDates defq = new SearchParamDates(resourceName, path, SearchParamTypes.valueOf(paramType),
						period.getStart(), period.getEnd());
				values.add(defq);

			} else {
				if (!multiType) {
					throw new SearchParameterException(
							"Search param " + resourceName + " is of unexpected datatype: " + obj.getClass());
				} else {
					continue;
				}
			}
		}
		return values;
	}

}
