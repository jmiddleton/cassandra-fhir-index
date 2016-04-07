package io.puntanegra.fhir.index.search.datatypes;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;

public class SearchParamNumber extends AbstractSearchParam {

	private Double value;

	public SearchParamNumber(String name, String path, SearchParamTypes type, Double value) {
		super(name, path, type);
		this.value = value;
	}

	@Override
	public boolean hasValue() {
		return this.value != null;
	}

	public Double getValue() {
		return this.value;
	}

	@Override
	public String getValueAsString() {
		if (this.value != null) {
			return this.value.toString();
		}
		return null;
	}

	@Override
	public List<Field> createIndexedFields() {
		Field field = new DoubleField(name, this.value, Field.Store.NO);
		field.setBoost(0.1f);
		return Arrays.asList(field);
	}
}
