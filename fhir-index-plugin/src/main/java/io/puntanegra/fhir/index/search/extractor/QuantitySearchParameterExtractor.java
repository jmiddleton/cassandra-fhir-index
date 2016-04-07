package io.puntanegra.fhir.index.search.extractor;

import java.util.HashSet;
import java.util.Set;

import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamQuantity;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Extracts information from generic attribute types.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class QuantitySearchParameterExtractor extends AbstractSearchParameterExtractor {

	public QuantitySearchParameterExtractor(FhirContext ctx) {
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

			if (obj instanceof Quantity) {
				Quantity quantity = (Quantity) obj;
				if (quantity.getValueElement().isEmpty()) {
					continue;
				}

				SearchParamQuantity defq = new SearchParamQuantity(resourceName, path,
						SearchParamTypes.valueOf(paramType), quantity.getValueElement().getValue().doubleValue(),
						quantity.getSystemElement().getValueAsString(), quantity.getCode());
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
