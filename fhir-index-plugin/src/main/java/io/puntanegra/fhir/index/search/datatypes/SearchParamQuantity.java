package io.puntanegra.fhir.index.search.datatypes;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;

public class SearchParamQuantity extends AbstractSearchParam {
	private Double value = null;
	private String system;
	private String code;

	public SearchParamQuantity(String name, String path, SearchParamTypes type, Double value, String system,
			String code) {
		super(name, path, type);
		this.value = value;
		this.system = system;
		this.code = code;
	}

	@Override
	public boolean hasValue() {
		return value != null;
	}

	public boolean hasSystem() {
		return this.system != null;
	}

	public boolean hasCode() {
		return this.code != null;
	}

	public Double getValue() {
		return this.value;
	}

	public void setValue(Double bd) {
		this.value = bd;
	}

	@Override
	public String getValueAsString() {
		if (this.value != null) {
			return this.value.toString();
		}
		return null;
	}

	public String getSystem() {
		return system;
	}

	public void setSystem(String system) {
		this.system = system;
	}

	public String getCode() {
		return code;
	}

	public void setCode(String code) {
		this.code = code;
	}

	@Override
	public List<Field> createIndexedFields() {
		List<Field> fields = new ArrayList<Field>();
		Field field = new DoubleField(this.name, this.value, Field.Store.NO);
		fields.add(field);

		if (hasSystem()) {
			field = new StringField(this.name + "_system", this.system, Field.Store.NO);
			fields.add(field);
		}

		if (hasCode()) {
			field = new StringField(this.name + "_code", this.code, Field.Store.NO);
			fields.add(field);
		}

		return fields;
	}

}
