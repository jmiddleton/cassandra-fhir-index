package io.puntanegra.fhir.index.search.extractor;

import ca.uhn.fhir.context.FhirContext;
import io.puntanegra.fhir.index.search.SearchParamExtractor;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

public class SearchParameterExtractorFactory {
	private static SearchParameterExtractorFactory instance;
	private FhirContext ctx;

	protected SearchParameterExtractorFactory(FhirContext ctx) {
		this.ctx = ctx;

	}

	public static SearchParameterExtractorFactory getInstance(FhirContext ctx) {
		if (instance == null) {
			instance = new SearchParameterExtractorFactory(ctx);
		}
		return instance;
	}

	public SearchParamExtractor getParameterExtractor(SearchParamTypes type) {
		if (SearchParamTypes.TOKEN == type) {
			return new TokenSearchParameterExtractor(this.ctx);
		} else if (SearchParamTypes.URI == type) {
			return new UriSearchParameterExtractor(this.ctx);
		} else if (SearchParamTypes.QUANTITY == type) {
			return new QuantitySearchParameterExtractor(this.ctx);
		} else if (SearchParamTypes.NUMBER == type) {
			return new NumberSearchParameterExtractor(this.ctx);
		} else if (SearchParamTypes.DATE == type) {
			return new DatesSearchParameterExtractor(this.ctx);
		} else {
			return new StringsSearchParameterExtractor(this.ctx);
		}
	}
}
