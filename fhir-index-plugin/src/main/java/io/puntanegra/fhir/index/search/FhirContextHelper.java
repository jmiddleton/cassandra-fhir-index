package io.puntanegra.fhir.index.search;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.FhirVersionEnum;

public class FhirContextHelper {

	private static FhirContext ctx;

	public static FhirContext getContext(FhirVersionEnum fhirVersion) {
		if (ctx == null) {
			ctx = new FhirContext(fhirVersion);
		}
		return ctx;
	}
}
