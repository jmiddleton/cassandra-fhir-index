package io.puntanegra.fhir.index.search;

import java.util.Set;

import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;

/**
 * Classes of this type are used to extract information from a Fhir
 * {@link IBaseResource}. <br>
 * Each type knows how to extract specific information based on the type.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public interface SearchParamExtractor {

	/**
	 * Extracts the search parameter values from the {@link IBaseResource}.
	 * 
	 * @param instance
	 * @param searchParam
	 * @return
	 */
	public Set<AbstractSearchParam> extractValues(IBaseResource instance, RuntimeSearchParam searchParam);
}
