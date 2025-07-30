package s10k.tool.domain;

import static net.solarnetwork.domain.datum.DatumSamplesType.Accumulating;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import net.solarnetwork.domain.datum.Datum;

/**
 * Validation information.
 */
public record DatumStreamValidationInfo(Datum expected, Datum actual) {

	/**
	 * Compute a map of property differences.
	 * 
	 * <p>
	 * Note that {@code null} values are treated like {@code 0}.
	 * </p>
	 * 
	 * @param accumulatingProperties the accumulating properties to extract
	 * @return the differences, or an empty map if there are none
	 */
	public Map<String, PropertyValueComparison> differences(String[] accumulatingProperties) {
		Map<String, PropertyValueComparison> result = null;
		for (String propertyName : accumulatingProperties) {
			BigDecimal expectedVal = (expected != null
					? expected.asSampleOperations().getSampleBigDecimal(Accumulating, propertyName)
					: null);
			BigDecimal actualVal = (actual != null
					? actual.asSampleOperations().getSampleBigDecimal(Accumulating, propertyName)
					: null);
			if (expectedVal == null && actualVal == null) {
				continue;
			}
			if (expectedVal == null) {
				expectedVal = BigDecimal.ZERO;
			}
			if (actualVal == null) {
				actualVal = BigDecimal.ZERO;
			}
			if (expectedVal.compareTo(actualVal) == 0) {
				continue;
			}
			if (result == null) {
				result = new LinkedHashMap<>(accumulatingProperties.length);
			}
			result.put(propertyName, new PropertyValueComparison(expectedVal, actualVal));
		}
		return result == null ? Map.of() : result;
	}

}
