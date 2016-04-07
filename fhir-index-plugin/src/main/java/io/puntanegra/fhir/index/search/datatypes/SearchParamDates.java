package io.puntanegra.fhir.index.search.datatypes;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongField;

public class SearchParamDates extends AbstractSearchParam {
	private Date low;
	private Date high;

	public SearchParamDates(String name, String path, SearchParamTypes type, Date low, Date high) {
		super(name, path, type);
		this.low = low;
		this.high = high;
	}

	@Override
	public boolean hasValue() {
		return low != null;
	}

	@Override
	public String getValueAsString() {
		// TODO Auto-generated method stub
		return null;
	}

	public Date getLow() {
		return low;
	}

	public void setLow(Date low) {
		this.low = low;
	}

	public Date getHigh() {
		return high;
	}

	public void setHigh(Date high) {
		this.high = high;
	}

	@Override
	public Date getValue() {
		return this.low;
	}

	@Override
	public List<Field> createIndexedFields() {
		Field fieldLow = new LongField(name, this.low.getTime(), Field.Store.NO);

		// TODO: ver como se busca por un periodo
		Field fieldHigh = null;
		if (this.high != null) {
			fieldHigh = new LongField(name + "_high", this.high.getTime(), Field.Store.NO);
		}

		return Arrays.asList(fieldLow, fieldHigh);
	}

}
