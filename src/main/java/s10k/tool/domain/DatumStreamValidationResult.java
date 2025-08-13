package s10k.tool.domain;

import java.time.Instant;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.threeten.extra.Interval;

import s10k.tool.support.IntervalSorter;

/**
 * A single datum stream's validation result.
 * 
 * @param nodeAndSource     the stream identifier
 * @param zone              the stream time zone
 * @param state             the validation execution state
 * @param invalidTimeRanges list of time ranges with discovered differences
 */
public record DatumStreamValidationResult(NodeAndSource nodeAndSource, ZoneId zone,
		AtomicReference<ValidationState> state, List<TimeRangeValidationDifference> invalidTimeRanges)
		implements Comparable<DatumStreamValidationResult> {

	/**
	 * Create an empty validation result.
	 * 
	 * @param nodeAndSource the datum stream identifier
	 * @param zone          the datum stream time zone
	 * @param state         the validation execution state
	 * @return the new instance
	 */
	public static DatumStreamValidationResult emptyValidationResult(NodeAndSource nodeAndSource, ZoneId zone,
			AtomicReference<ValidationState> state) {
		return new DatumStreamValidationResult(nodeAndSource, zone, state, List.of());
	}

	@Override
	public int compareTo(DatumStreamValidationResult o) {
		return nodeAndSource.compareTo(o.nodeAndSource);
	}

	/**
	 * Get the validation state.
	 * 
	 * @return the state
	 */
	public ValidationState validationState() {
		return state.get();
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
	public SortedSet<Interval> uniqueHourTimeRanges() {
		// mapping of start date -> range
		Map<Instant, Interval> forward = new HashMap<>();

		// mapping of end date -> range
		Map<Instant, Interval> reverse = new HashMap<>();

		for (TimeRangeValidationDifference validation : invalidTimeRanges) {
			if (!validation.isHourRange()) {
				continue;
			}
			Interval adjacentAfter = forward.remove(validation.range().getEnd());
			Interval adjacentBefore = reverse.remove(validation.range().getStart());
			if (adjacentAfter != null) {
				reverse.remove(adjacentAfter.getEnd());
			}
			if (adjacentBefore != null) {
				forward.remove(adjacentBefore.getStart());
			}
			Interval mergedRange = null;
			if (adjacentBefore != null && adjacentAfter != null) {
				// merge two ranges into one
				mergedRange = Interval.of(adjacentBefore.getStart(), adjacentAfter.getEnd());
			} else if (adjacentBefore != null) {
				// merge before into current
				mergedRange = Interval.of(adjacentBefore.getStart(), validation.range().getEnd());
			} else if (adjacentAfter != null) {
				// mege after into current
				mergedRange = Interval.of(validation.range().getStart(), adjacentAfter.getEnd());
			} else {
				// new range
				forward.put(validation.range().getStart(), validation.range());
				reverse.put(validation.range().getEnd(), validation.range());
			}
			if (mergedRange != null) {
				forward.put(mergedRange.getStart(), mergedRange);
				reverse.put(mergedRange.getEnd(), mergedRange);
			}
		}
		var result = new TreeSet<Interval>(IntervalSorter.INSTANCE);
		result.addAll(forward.values());
		return result;
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
				.sorted((l, r) ->  IntervalSorter.INSTANCE.compare(l.range(), r.range()))
				.toList();
		// @formatter:on
	}

}
