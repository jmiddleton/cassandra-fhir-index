package io.puntanegra.fhir.index.search.datatypes;

/**
 * Search parameter types. These types are the same defined in HAPI
 * RestSearchParameterTypeEnum.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public enum SearchParamTypes {
	NUMBER, DATE, STRING, TOKEN, REFERENCE, COMPOSITE, QUANTITY, URI;
}
