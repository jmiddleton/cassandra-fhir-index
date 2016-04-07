package io.puntanegra.fhir.index.search.datatypes;

import java.util.Arrays;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

public class SearchParamString extends AbstractSearchParam {

	private String value;

	public SearchParamString(String name, String path, SearchParamTypes type, String value) {
		super(name, path, type);
		this.value = value;
	}

	@Override
	public boolean hasValue() {
		return value != null;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String str) {
		this.value = str;
	}

	public String getValueAsString() {
		return this.value;
	}

	@Override
	public List<Field> createIndexedFields() {
		Field field = new TextField(this.name, this.value, Field.Store.NO);
		return Arrays.asList(field);
	}
}
