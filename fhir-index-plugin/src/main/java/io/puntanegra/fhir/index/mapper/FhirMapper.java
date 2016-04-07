package io.puntanegra.fhir.index.mapper;

import java.util.List;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.FhirVersionEnum;
import io.puntanegra.fhir.index.config.ResourceOptions;
import io.puntanegra.fhir.index.search.SearchParamExtractorHelper;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamString;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Mapper class used to create Lucene {@link Document} index from a FHIR
 * {@link IBaseResource}. <br>
 * Based on the index configuration, it extracts search parameter values from a
 * FHIR {@link IBaseResource} and creates the appropiate Lucene {@link Field}.
 * The fields are indexed and can be sorted based on the configuration.
 *
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class FhirMapper {

	private ResourceOptions searchOptions;

	private SearchParamExtractorHelper fhirExtractor;

	public FhirMapper(ResourceOptions searchOptions) {
		this.searchOptions = searchOptions;
		this.fhirExtractor = new SearchParamExtractorHelper(FhirVersionEnum.DSTU3);
	}

	/**
	 * Parses Json FHIR Resource and converts to Lucene {@link Field}s.
	 * 
	 * @param document,
	 *            the Lucene {@link Document}.
	 * @param json,
	 *            the FHIR resource as JSON format.
	 */
	public void addFields(Document document, String json) {

		IBaseResource resourceInstance = this.fhirExtractor.parseResource(json);
		String resourceName = resourceInstance.getClass().getSimpleName();

		Set<String> parameters = this.searchOptions.resources.get(resourceName);
		Set<AbstractSearchParam> values = this.fhirExtractor.extractParametersValues(resourceInstance, parameters);
		for (AbstractSearchParam entry : values) {
			doAddFields(document, entry, false);
		}

		doAddFields(document, new SearchParamString("resource_type", "", SearchParamTypes.STRING, resourceName), false);

	}

	/**
	 * Adds the specified column name and value to a Lucene {@link Document}.
	 * The added fields are indexed and sorted (if the parameter sorted is
	 * <code>true</code>).
	 *
	 * @param document
	 *            a {@link Document}
	 * @param value
	 *            the parameter to add to a {@link Document}
	 * @param sorted
	 *            sort the value or not
	 */
	private void doAddFields(Document document, AbstractSearchParam value, boolean sorted) {
		if (value != null && value.hasValue()) {
			doAddIndexedFields(document, value);

			if (sorted) {
				doAddSortedFields(document, value);
			}
		}
	}

	/**
	 * Add a {@link Field} to be sorted by.
	 * 
	 * @param document
	 * @param param
	 */
	private void doAddSortedFields(Document document, AbstractSearchParam param) {
		// BytesRef bytes = new BytesRef(value);
		// document.add(new SortedDocValuesField(value.getName(), bytes));

		// TODO: implement sort
		List<Field> fields = param.createSortedFields();
		for (Field field : fields) {
			if (field != null) {
				document.add(field);
			}
		}
	}

	/**
	 * Add an indexed {@link Field}.
	 * 
	 * @param document
	 * @param param
	 */
	private void doAddIndexedFields(Document document, AbstractSearchParam param) {
		List<Field> fields = param.createIndexedFields();
		for (Field field : fields) {
			if (field != null) {
				document.add(field);
			}
		}
	}

}
