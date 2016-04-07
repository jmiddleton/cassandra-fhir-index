package io.puntanegra.fhir.index.search.extractor;

import java.util.ArrayList;
import java.util.List;

import org.hl7.fhir.dstu3.model.Enumeration;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeResourceDefinition;
import ca.uhn.fhir.context.RuntimeSearchParam;
import ca.uhn.fhir.util.FhirTerser;
import io.puntanegra.fhir.index.search.SearchParamExtractor;
import io.puntanegra.fhir.index.search.datatypes.SearchParamString;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

public abstract class AbstractSearchParameterExtractor implements SearchParamExtractor {

	protected static final Logger logger = LoggerFactory.getLogger(SearchParamExtractor.class);
	protected static final int MAX_LENGTH = 200;

	protected FhirContext ctx;

	public AbstractSearchParameterExtractor(FhirContext ctx) {
		this.ctx = ctx;
	}

	protected List<Object> extractValues(String paths, IBaseResource instance) {
		List<Object> values = new ArrayList<Object>();
		String[] nextPathsSplit = paths.split("\\|");
		FhirTerser t = this.ctx.newTerser();
		for (String nextPath : nextPathsSplit) {
			String nextPathTrimmed = nextPath.trim();
			try {
				values.addAll(t.getValues(instance, nextPathTrimmed));
			} catch (Exception e) {
				RuntimeResourceDefinition def = this.ctx.getResourceDefinition(instance);
				logger.warn("Failed to index values from path[{}] in resource type[{}]: {}",
						new Object[] { nextPathTrimmed, def.getName(), e.toString(), e });
			}
		}
		return values;
	}

	protected static <T extends Enum<?>> String extractSystem(Enumeration<T> theBoundCode) {
		if (theBoundCode.getValue() != null) {
			return theBoundCode.getEnumFactory().toSystem(theBoundCode.getValue());
		}
		return null;
	}

	/**
	 * Create a {@link SearchParamString} value.
	 * 
	 * @param searchParam
	 * @param value
	 * @return
	 */
	protected SearchParamString addStringParam(RuntimeSearchParam searchParam, String value) {
		String path = searchParam.getPath();
		String resourceName = searchParam.getName();

		return new SearchParamString(resourceName, path, SearchParamTypes.STRING, value);
	}

	protected String getParamType(RuntimeSearchParam searchParam) {
		String paramType = searchParam.getParamType().getCode().toUpperCase();
		return paramType;
	}

}
