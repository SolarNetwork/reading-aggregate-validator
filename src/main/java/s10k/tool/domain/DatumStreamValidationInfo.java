package s10k.tool.domain;

import static net.solarnetwork.domain.datum.DatumSamplesType.Accumulating;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import net.solarnetwork.domain.datum.Datum;

/**
 * Validation information.
 */
public record DatumStreamValidationInfo(Datum expected, Datum actual, String[] propertyNames) {

	/**
	 * Compute a map of property differences.
	 * 
	 * @return the differences, or an empty map if there are none
	 */
	public Map<String, PropertyValueComparison> differences() {
		Map<String, PropertyValueComparison> result = null;
		for (String propertyName : propertyNames) {
			BigDecimal expectedVal = expected.asSampleOperations().getSampleBigDecimal(Accumulating, propertyName);
			BigDecimal actualVal = actual.asSampleOperations().getSampleBigDecimal(Accumulating, propertyName);
			if (expectedVal == null && actualVal == null) {
				continue;
			}
			if (expectedVal != null && actualVal != null && expectedVal.compareTo(actualVal) == 0) {
				continue;
			}
			if (result == null) {
				result = new LinkedHashMap<>(propertyNames.length);
			}
			result.put(propertyName, new PropertyValueComparison(expectedVal, actualVal));
		}
		return result == null ? Map.of() : result;
	}

}
