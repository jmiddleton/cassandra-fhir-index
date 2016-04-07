package io.puntanegra.fhir.index.search.datatypes;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.document.Field;

public abstract class AbstractSearchParam {

	static final int MAX_SP_NAME = 200;

	protected String name;
	protected String path;
	protected SearchParamTypes type;

	public AbstractSearchParam() {

	}

	public AbstractSearchParam(String name, String path, SearchParamTypes type) {
		super();
		this.name = name;
		this.path = path;
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public String getPath() {
		return path;
	}

	public SearchParamTypes getType() {
		return type;
	}

	public String getTypeAsString() {
		return type.name();
	}

	public void setTypeAsString(String type) {
		this.type = SearchParamTypes.valueOf(type);
	}

	@Override
	public String toString() {
		return "ResourceSearchParameters [name=" + name + ", path=" + path + ", type=" + type + "]";
	}

	// TODO: finish this code
	public String getTypeAsCassandraDataType() {
		if (this.type == SearchParamTypes.NUMBER) {
			return "double";
		} else if (this.type == SearchParamTypes.QUANTITY) {
			return "double";
		} else if (this.type == SearchParamTypes.DATE) {
			return "timestamp";
		} else {
			return "text";
		}
	}

	public abstract boolean hasValue();

	public abstract String getValueAsString();

	public abstract Object getValue();

	public abstract List<Field> createIndexedFields();

	public List<Field> createSortedFields() {
		return Collections.emptyList();
	};
}
