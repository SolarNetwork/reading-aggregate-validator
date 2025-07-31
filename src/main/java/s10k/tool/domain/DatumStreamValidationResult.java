package s10k.tool.domain;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * A single datum stream's validation result.
 */
public record DatumStreamValidationResult(NodeAndSource nodeAndSource, ZoneId zone,
		List<TimeRangeValidationDifference> invalidTimeRanges) {

	/**
	 * Create an empty validation result.
	 * 
	 * @param nodeAndSource the datum stream identifier
	 * @param zone          the datum stream time zone
	 * @return the new instance
	 */
	public static DatumStreamValidationResult emptyValidationResult(NodeAndSource nodeAndSource, ZoneId zone) {
		return new DatumStreamValidationResult(nodeAndSource, zone, List.of());
	}

	/**
	 * Test if any differences are available.
	 * 
	 * @return {@code true} if any differences are available
	 */
	public boolean hasDifferences() {
		return !invalidTimeRanges.isEmpty();
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
	public SortedSet<LocalDateTimeRange> uniqueHourTimeRanges() {
		// mapping of start date -> range
		Map<LocalDateTime, LocalDateTimeRange> forward = new HashMap<>();

		// mapping of end date -> range
		Map<LocalDateTime, LocalDateTimeRange> reverse = new HashMap<>();

		for (TimeRangeValidationDifference validation : invalidTimeRanges) {
			if (!validation.isHourRange()) {
				continue;
			}
			LocalDateTimeRange adjacentAfter = forward.remove(validation.timeRange().end());
			LocalDateTimeRange adjacentBefore = reverse.remove(validation.timeRange().start());
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
				mergedRange = new LocalDateTimeRange(adjacentBefore.start(), validation.timeRange().end());
			} else if (adjacentAfter != null) {
				// mege after into current
				mergedRange = new LocalDateTimeRange(validation.timeRange().start(), adjacentAfter.end());
			} else {
				// new range
				forward.put(validation.timeRange().start(), validation.timeRange());
				reverse.put(validation.timeRange().end(), validation.timeRange());
			}
			if (mergedRange != null) {
				forward.put(mergedRange.start(), mergedRange);
				reverse.put(mergedRange.end(), mergedRange);
			}
		}
		return new TreeSet<>(forward.values());
	}

	/**
	 * Extract the list of hour-level validation differences.
	 * 
	 * @return the hour-level validation differences
	 */
	public List<TimeRangeValidationDifference> invalidHours() {
		// @formatter:off
		return invalidTimeRanges.stream()
				.filter(TimeRangeValidationDifference::isHourRange)
				.sorted((l, r) -> l.timeRange().compareTo(r.timeRange()))
				.toList();
		// @formatter:on
	}

}
