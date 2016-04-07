package io.puntanegra.fhir.index.search;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;
import io.puntanegra.fhir.index.search.extractor.SearchParameterExtractorFactory;

/**
 * Helper class to extract information from FHIR {@link IBaseResource}. <br>
 * Based on the search parameters metadata defined in the resource, it extracts
 * instance information which is later used to index a particular resource.<br>
 * If no parameter is defined, then all the search parameters will be used for
 * indexing.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class SearchParamExtractorHelper {

	private FhirVersionEnum fhirVersion = FhirVersionEnum.DSTU3;
	private FhirContext ctx;
	private SearchParameterExtractorFactory extractorFactory;

	public SearchParamExtractorHelper(FhirVersionEnum version) {
		this.fhirVersion = version;
		this.ctx = FhirContextHelper.getContext(this.fhirVersion);
		this.extractorFactory = SearchParameterExtractorFactory.getInstance(this.ctx);
	}

	/**
	 * Extracts search parameter metadata defined in a resource.
	 * 
	 * @param instance,
	 *            the FHIR {@link IBaseResource}
	 * @param parameters,
	 *            parameters to index for this resource. This are defined during
	 *            index creation.<br>
	 *            If no parameter is defined, then all the FHIR search
	 *            parameters will be extracted.
	 * @return
	 */
	public Set<AbstractSearchParam> extractParametersValues(IBaseResource instance, Set<String> parameters) {
		Set<AbstractSearchParam> values = new HashSet<AbstractSearchParam>();

		RuntimeResourceDefinition def = this.ctx.getResourceDefinition(instance);

		// si no hay parametros definidos, indexamos todos los atributos
		// definidos en el recurso FHIR.
		if (parameters == null || parameters.isEmpty()) {
			List<RuntimeSearchParam> params = def.getSearchParams();
			for (RuntimeSearchParam searchParam : params) {
				values.addAll(doCreateSearchParam(instance, searchParam));
			}
		} else {
			for (String param : parameters) {
				RuntimeSearchParam searchParam = def.getSearchParam(param);
				values.addAll(doCreateSearchParam(instance, searchParam));

			}
		}
		return values;
	}

	private Set<AbstractSearchParam> doCreateSearchParam(IBaseResource instance, RuntimeSearchParam searchParam) {
		String nextPath = searchParam.getPath();
		if (isBlank(nextPath)) {
			return Collections.emptySet();
		}

		String strType = searchParam.getParamType().getCode().toUpperCase();

		SearchParamExtractor extractor = extractorFactory.getParameterExtractor(SearchParamTypes.valueOf(strType));
		return extractor.extractValues(instance, searchParam);
	}

	@SuppressWarnings("unchecked")
	public IBaseResource parseResource(String resourceType, String json) throws ClassNotFoundException {
		Class<IBaseResource> type = (Class<IBaseResource>) Class.forName(resourceType).asSubclass(IBaseResource.class);
		return ctx.newJsonParser().parseResource(type, json);
	}

	/**
	 * Parse a JSON document to a FHIR {@link IBaseResource}.
	 * 
	 * @param json
	 * @return
	 */
	public IBaseResource parseResource(String json) {
		return ctx.newJsonParser().parseResource(json);
	}

}
