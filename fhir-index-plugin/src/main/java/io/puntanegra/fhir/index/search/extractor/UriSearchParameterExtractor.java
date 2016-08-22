package io.puntanegra.fhir.index.search.extractor;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.HashSet;
import java.util.Set;

import org.hl7.fhir.dstu3.model.Reference;
import org.hl7.fhir.dstu3.model.UriType;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamString;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Extracts information from generic attribute types.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class UriSearchParameterExtractor extends AbstractSearchParameterExtractor {

	public UriSearchParameterExtractor(FhirContext ctx) {
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

			if (obj instanceof UriType) {
				UriType uri = (UriType) obj;
				if (isBlank(uri.getValue())) {
					continue;
				}

				logger.trace("Adding param: {}, {}", resourceName, uri.getValue());

				SearchParamString def = new SearchParamString(resourceName, path, SearchParamTypes.valueOf(paramType),
						uri.getValue());
				values.add(def);
			} else if (obj instanceof Reference) {
				Reference ref = (Reference) obj;
				SearchParamString def = new SearchParamString(resourceName, path, SearchParamTypes.valueOf(paramType),
						ref.getReference());
				values.add(def);
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
