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
 * 
 * @param nodeAndSource     the stream identifier
 * @param zone              the stream time zone
 * @param invalidTimeRanges list of time ranges with discovered differences
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
				mergedRange = new LocalDateTimeRange(validation.range().start(), adjacentAfter.end());
			} else {
				// new range
				forward.put(validation.range().start(), validation.range());
				reverse.put(validation.range().end(), validation.range());
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
				.sorted((l, r) -> l.range().compareTo(r.range()))
				.toList();
		// @formatter:on
	}

}
