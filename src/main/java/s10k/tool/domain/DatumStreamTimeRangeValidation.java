package s10k.tool.domain;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A datum stream time range with associated validation information.
 */
public record DatumStreamTimeRangeValidation(DatumStreamTimeRange range, DatumStreamValidationInfo validationInfo,
		Map<String, PropertyValueComparison> differences) {

	/**
	 * Test if this validation represents a single hour.
	 * 
	 * @return {@code true} if the range represents a single hour
	 */
	public boolean isHourRange() {
		return range.hourCount() == 1L;
	}

	/**
	 * Get an ordered set of the minimal non-overlapping hour-level time ranges in a
	 * set of validations.
	 * 
	 * <p>
	 * This method assumes a single datum stream is represented in all
	 * {@code validations}.
	 * </p>
	 * 
	 * @param validations the validations to inspect
	 * @return the ordered set of time ranges
	 */
	public static SortedSet<LocalDateTimeRange> uniqueHourTimeRanges(
			Collection<DatumStreamTimeRangeValidation> validations) {
		// mapping of start date -> range
		Map<LocalDateTime, LocalDateTimeRange> forward = new HashMap<>();

		// mapping of end date -> range
		Map<LocalDateTime, LocalDateTimeRange> reverse = new HashMap<>();

		for (DatumStreamTimeRangeValidation validation : validations) {
			if (!validation.isHourRange()) {
				continue;
			}
			LocalDateTimeRange adjacentAfter = forward.remove(validation.range().end());
			LocalDateTimeRange adjacentBefore = reverse.remove(validation.range().start());
			if (adjacentAfter != null) {
				reverse.remove(adjacentAfter.end());
			}
			if (adjacentBefore != null) {
				forward.remove(adjacentBefore.start());
			}
			LocalDateTimeRange mergedRange = null;
			if (adjacentBefore != null && adjacentAfter != null) {
				// merge two ranges into one
				mergedRange = new LocalDateTimeRange(adjacentBefore.start(), adjacentAfter.end());
			} else if (adjacentBefore != null) {
				// merge before into current
				mergedRange = new LocalDateTimeRange(adjacentBefore.start(), validation.range().end());
			} else if (adjacentAfter != null) {
				// mege after into current
				mergedRange = new LocalDateTimeRange(validation.range.start(), adjacentAfter.end());
			} else {
				// new range
				forward.put(validation.range.start(), validation.range.timeRange());
				reverse.put(validation.range.end(), validation.range.timeRange());
			}
			if (mergedRange != null) {
				forward.put(mergedRange.start(), mergedRange);
				reverse.put(mergedRange.end(), mergedRange);
			}
		}
		return new TreeSet<>(forward.values());
	}
}
