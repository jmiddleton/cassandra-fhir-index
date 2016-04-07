package io.puntanegra.fhir.index.search.extractor;

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.HashSet;
import java.util.Set;

import javax.measure.unit.NonSI;
import javax.measure.unit.Unit;

import org.hl7.fhir.dstu3.model.Duration;
import org.hl7.fhir.dstu3.model.IntegerType;
import org.hl7.fhir.dstu3.model.Quantity;
import org.hl7.fhir.instance.model.api.IBase;
import org.hl7.fhir.instance.model.api.IBaseResource;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.context.RuntimeSearchParam;
import io.puntanegra.fhir.index.search.datatypes.AbstractSearchParam;
import io.puntanegra.fhir.index.search.datatypes.SearchParamNumber;
import io.puntanegra.fhir.index.search.datatypes.SearchParamTypes;

/**
 * Extracts information from generic attribute types.
 * 
 * @author Jorge L. Middleton {@literal <jorge.middleton@gmail.com>}
 *
 */
public class NumberSearchParameterExtractor extends AbstractSearchParameterExtractor {

	public static final String UCUM_NS = "http://unitsofmeasure.org";

	public NumberSearchParameterExtractor(FhirContext ctx) {
		super(ctx);
	}

	@Override
	public Set<AbstractSearchParam> extractValues(IBaseResource instance, RuntimeSearchParam searchParam) {

		Set<AbstractSearchParam> values = new HashSet<AbstractSearchParam>();

		String path = searchParam.getPath();
		String resourceName = searchParam.getName();
		String paramType = getParamType(searchParam);

		for (Object obj : extractValues(path, instance)) {
			if (obj == null || ((IBase) obj).isEmpty()) {
				continue;
			}

			boolean multiType = false;
			if (path.endsWith("[x]")) {
				multiType = true;
			}

			if (obj instanceof Duration) {
				Duration nextValue = (Duration) obj;
				if (nextValue.getValueElement().isEmpty()) {
					continue;
				}

				if (UCUM_NS.equals(nextValue.getSystem())) {
					if (isNotBlank(nextValue.getCode())) {
						Unit<? extends javax.measure.quantity.Quantity> unit = Unit.valueOf(nextValue.getCode());
						javax.measure.converter.UnitConverter dayConverter = unit.getConverterTo(NonSI.DAY);
						double dayValue = dayConverter.convert(nextValue.getValue().doubleValue());
						Duration newValue = new Duration();
						newValue.setSystem(UCUM_NS);
						newValue.setCode(NonSI.DAY.toString());
						newValue.setValue(dayValue);
						nextValue = newValue;
					}
				}

				SearchParamNumber defq = new SearchParamNumber(resourceName, path, SearchParamTypes.valueOf(paramType),
						nextValue.getValue().doubleValue());
				values.add(defq);
			} else if (obj instanceof Quantity) {
				Quantity nextValue = (Quantity) obj;
				if (nextValue.getValueElement().isEmpty()) {
					continue;
				}

				SearchParamNumber defq = new SearchParamNumber(resourceName, path, SearchParamTypes.valueOf(paramType),
						nextValue.getValue().doubleValue());
				values.add(defq);
			} else if (obj instanceof IntegerType) {
				IntegerType nextValue = (IntegerType) obj;
				if (nextValue.getValue() == null) {
					continue;
				}

				SearchParamNumber defq = new SearchParamNumber(resourceName, path, SearchParamTypes.valueOf(paramType),
						nextValue.getValue().doubleValue());
				values.add(defq);
			} else {
				if (!multiType) {
					throw new SearchParameterException(
							"Search param " + resourceName + " is of unexpected datatype: " + obj.getClass());
				} else {
					continue;
				}
			}
		}
		return values;
	}

}
