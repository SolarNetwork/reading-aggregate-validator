package s10k.tool.domain;

import static net.solarnetwork.domain.datum.DatumSamplesType.Accumulating;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import net.solarnetwork.domain.datum.Aggregation;
import net.solarnetwork.domain.datum.Datum;

/**
 * Validation differences found on a time range.
 * 
 * @param aggregation the aggregation level the validation was performed at
 * @param timeRange   the time range validated
 * @param differences the discovered property differences
 */
public record TimeRangeValidationDifference(Aggregation aggregation, LocalDateTimeRange range,
		Map<String, PropertyValueComparison> differences) {

	/**
	 * Create a time range validation difference from expected and actual
	 * {@link Datum}.
	 * 
	 * <p>
	 * Note that {@code null} values are treated like {@code 0}.
	 * </p>
	 * 
	 * @param aggregation            the aggregation
	 * @param timeRange              the time range
	 * @param expected               the expected value
	 * @param actual                 the actual value
	 * @param accumulatingProperties the accumulating properties to compare
	 * @return the differences
	 */
	public static TimeRangeValidationDifference differences(Aggregation aggregation, LocalDateTimeRange timeRange,
			Datum expected, Datum actual, String[] accumulatingProperties) {
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
		return new TimeRangeValidationDifference(aggregation, timeRange, result == null ? Map.of() : result);
	}

	/**
	 * Test if any differences are available.
	 * 
	 * @return {@code true} if any differences are available
	 */
	public boolean hasDifferences() {
		return !differences.isEmpty();
	}

	/**
	 * Test if this validation represents a single hour.
	 * 
	 * @return {@code true} if the range represents a single hour
	 */
	public boolean isHourRange() {
		return range.hourCount() == 1L;
	}

}
