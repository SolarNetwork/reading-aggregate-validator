package s10k.tool.domain.test;

import static org.assertj.core.api.BDDAssertions.then;

import java.time.LocalDateTime;
import java.util.List;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;

import s10k.tool.domain.DatumStreamValidationResult;
import s10k.tool.domain.LocalDateTimeRange;
import s10k.tool.domain.TimeRangeValidationDifference;

/**
 * Test cases for the {@link DatumStreamValidationResult} class.
 */
public class DatumStreamValidationResultTests {

	private TimeRangeValidationDifference valHour(String startDate) {
		LocalDateTime start = LocalDateTime.parse(startDate);
		return val(start, start.plusHours(1));
	}

	private TimeRangeValidationDifference val(String startDate, String endDate) {
		LocalDateTime start = LocalDateTime.parse(startDate);
		LocalDateTime end = LocalDateTime.parse(endDate);
		return val(start, end);
	}

	private TimeRangeValidationDifference val(LocalDateTime start, LocalDateTime end) {
		return new TimeRangeValidationDifference(null, new LocalDateTimeRange(start, end), null);
	}

	private DatumStreamValidationResult result(List<TimeRangeValidationDifference> diffs) {
		return new DatumStreamValidationResult(null, null, diffs);
	}

	@Test
	public void uniqueHourTimeRanges_nothingOverlaps() {
		// GIVEN
		// @formatter:off
		List<TimeRangeValidationDifference> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T00:00")
					, valHour("2025-01-01T03:00")
					, valHour("2025-01-01T06:00")
					, valHour("2025-01-01T09:00")
				);
		// @formatter:on
		DatumStreamValidationResult validation = result(validations);

		// WHEN
		SortedSet<LocalDateTimeRange> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("All hour ranges preserved")
			.containsExactly(
					  validations.get(1).timeRange()
					, validations.get(2).timeRange()
					, validations.get(3).timeRange()
					, validations.get(4).timeRange()
			)
			;
		// @formatter:off
	}

	@Test
	public void uniqueHourTimeRanges_nothingOverlaps_sorted() {
		// GIVEN
		// @formatter:off
		List<TimeRangeValidationDifference> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T09:00")
					, valHour("2025-01-01T03:00")
					, valHour("2025-01-01T00:00")
					, valHour("2025-01-01T06:00")
				);
		// @formatter:on
		DatumStreamValidationResult validation = result(validations);

		// WHEN
		SortedSet<LocalDateTimeRange> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("All hour ranges preserved and sorted by time")
			.containsExactly(
					  validations.get(3).timeRange()
					, validations.get(2).timeRange()
					, validations.get(4).timeRange()
					, validations.get(1).timeRange()
			)
			;
		// @formatter:off
	}

	@Test
	public void uniqueHourTimeRanges_adjacents() {
		// GIVEN
		// @formatter:off
		List<TimeRangeValidationDifference> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T09:00")
					, valHour("2025-01-01T03:00")
					, valHour("2025-01-01T08:00")
					, valHour("2025-01-01T04:00")
				);
		// @formatter:on
		DatumStreamValidationResult validation = result(validations);

		// WHEN
		SortedSet<LocalDateTimeRange> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("Adjacent hours merged")
			.containsExactly(
					  new LocalDateTimeRange(validations.get(2).timeRange().start(), validations.get(4).timeRange().end())
					, new LocalDateTimeRange(validations.get(3).timeRange().start(), validations.get(1).timeRange().end())
			)
			;
		// @formatter:off
	}

	@Test
	public void uniqueHourTimeRanges_filledGap() {
		// GIVEN
		// @formatter:off
		List<TimeRangeValidationDifference> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T09:00")
					, valHour("2025-01-01T07:00")
					, valHour("2025-01-01T08:00")
					, valHour("2025-01-01T10:00")
				);
		// @formatter:on
		DatumStreamValidationResult validation = result(validations);

		// WHEN
		SortedSet<LocalDateTimeRange> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("Adjacent hours merged with gap filled")
			.containsExactly(
					  new LocalDateTimeRange(validations.get(2).timeRange().start(), validations.get(4).timeRange().end())
			)
			;
		// @formatter:off
	}

}
