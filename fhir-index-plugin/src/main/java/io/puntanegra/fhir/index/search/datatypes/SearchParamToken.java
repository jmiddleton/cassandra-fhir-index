package io.puntanegra.fhir.index.search.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

public class SearchParamToken extends AbstractSearchParam {

	private String system;
	private String code;

	public SearchParamToken(String name, String path, SearchParamTypes type, String system, String code) {
		super(name, path, type);
		this.system = system;
		this.code = code;
	}

	@Override
	public boolean hasValue() {
		return this.code != null;
	}

	public String getSystem() {
		return this.system;
	}

	public void setSystem(String str) {
		this.system = str;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	public String getValueAsString() {
		String str = "";
		if (this.system != null) {
			str = this.system;
		}
		if (this.code != null) {
			str = str + this.code;
		}
		return str;
	}

	@Override
	public String getValue() {
		return getValueAsString();
	}

	public boolean hasSystem() {
		return this.system != null;
	}

	@Override
	public List<Field> createIndexedFields() {
		List<Field> fields = new ArrayList<Field>();

		Field field;
		if (hasSystem()) {
			field = new StringField(this.name + "_system", this.system, Field.Store.NO);
			fields.add(field);
		}

		field = new StringField(this.name, this.code, Field.Store.NO);
		fields.add(field);

		return fields;
	}
}
