package s10k.tool.domain.test;

import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.BDDAssertions.then;

import java.time.LocalDateTime;
import java.util.List;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;
import org.threeten.extra.Interval;

import s10k.tool.domain.DatumStreamValidationResult;
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
		return new TimeRangeValidationDifference(null,
				Interval.of(start.atOffset(UTC).toInstant(), end.atOffset(UTC).toInstant()), null);
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
		SortedSet<Interval> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("All hour ranges preserved")
			.containsExactly(
					  validations.get(1).range()
					, validations.get(2).range()
					, validations.get(3).range()
					, validations.get(4).range()
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
		SortedSet<Interval> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("All hour ranges preserved and sorted by time")
			.containsExactly(
					  validations.get(3).range()
					, validations.get(2).range()
					, validations.get(4).range()
					, validations.get(1).range()
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
		SortedSet<Interval> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("Adjacent hours merged")
			.containsExactly(
					  Interval.of(validations.get(2).range().getStart(), validations.get(4).range().getEnd())
					, Interval.of(validations.get(3).range().getStart(), validations.get(1).range().getEnd())
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
		SortedSet<Interval> result = validation.uniqueHourTimeRanges();

		// THEN
		// @formatter:off
		then(result)
			.as("Adjacent hours merged with gap filled")
			.containsExactly(
					Interval.of(validations.get(2).range().getStart(), validations.get(4).range().getEnd())
			)
			;
		// @formatter:off
	}

}
