package s10k.tool.domain.test;

import static org.assertj.core.api.BDDAssertions.then;

import java.time.LocalDateTime;
import java.util.List;
import java.util.SortedSet;

import org.junit.jupiter.api.Test;

import s10k.tool.domain.DatumStreamTimeRange;
import s10k.tool.domain.DatumStreamTimeRangeValidation;
import s10k.tool.domain.LocalDateTimeRange;

/**
 * Test cases for the {@link DatumStreamTimeRangeValidation} class.
 */
public class DatumStreamTimeRangeValidationTests {

	private DatumStreamTimeRangeValidation valHour(String startDate) {
		LocalDateTime start = LocalDateTime.parse(startDate);
		return val(start, start.plusHours(1));
	}

	private DatumStreamTimeRangeValidation val(String startDate, String endDate) {
		LocalDateTime start = LocalDateTime.parse(startDate);
		LocalDateTime end = LocalDateTime.parse(endDate);
		return val(start, end);
	}

	private DatumStreamTimeRangeValidation val(LocalDateTime start, LocalDateTime end) {
		return new DatumStreamTimeRangeValidation(
				new DatumStreamTimeRange(null, null, new LocalDateTimeRange(start, end)), null, null);
	}

	@Test
	public void uniqueHourTimeRanges_nothingOverlaps() {
		// GIVEN
		// @formatter:off
		List<DatumStreamTimeRangeValidation> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T00:00")
					, valHour("2025-01-01T03:00")
					, valHour("2025-01-01T06:00")
					, valHour("2025-01-01T09:00")
				);
		// @formatter:on

		// WHEN
		SortedSet<LocalDateTimeRange> result = DatumStreamTimeRangeValidation.uniqueHourTimeRanges(validations);

		// THEN
		// @formatter:off
		then(result)
			.as("All hour ranges preserved")
			.containsExactly(
					  validations.get(1).range().timeRange()
					, validations.get(2).range().timeRange()
					, validations.get(3).range().timeRange()
					, validations.get(4).range().timeRange()
			)
			;
		// @formatter:off
	}

	@Test
	public void uniqueHourTimeRanges_nothingOverlaps_sorted() {
		// GIVEN
		// @formatter:off
		List<DatumStreamTimeRangeValidation> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T09:00")
					, valHour("2025-01-01T03:00")
					, valHour("2025-01-01T00:00")
					, valHour("2025-01-01T06:00")
				);
		// @formatter:on

		// WHEN
		SortedSet<LocalDateTimeRange> result = DatumStreamTimeRangeValidation.uniqueHourTimeRanges(validations);

		// THEN
		// @formatter:off
		then(result)
			.as("All hour ranges preserved and sorted by time")
			.containsExactly(
					  validations.get(3).range().timeRange()
					, validations.get(2).range().timeRange()
					, validations.get(4).range().timeRange()
					, validations.get(1).range().timeRange()
			)
			;
		// @formatter:off
	}

	@Test
	public void uniqueHourTimeRanges_adjacents() {
		// GIVEN
		// @formatter:off
		List<DatumStreamTimeRangeValidation> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T09:00")
					, valHour("2025-01-01T03:00")
					, valHour("2025-01-01T08:00")
					, valHour("2025-01-01T04:00")
				);
		// @formatter:on

		// WHEN
		SortedSet<LocalDateTimeRange> result = DatumStreamTimeRangeValidation.uniqueHourTimeRanges(validations);

		// THEN
		// @formatter:off
		then(result)
			.as("Adjacent hours merged")
			.containsExactly(
					  new LocalDateTimeRange(validations.get(2).range().start(), validations.get(4).range().end())
					, new LocalDateTimeRange(validations.get(3).range().start(), validations.get(1).range().end())
			)
			;
		// @formatter:off
	}

	@Test
	public void uniqueHourTimeRanges_filledGap() {
		// GIVEN
		// @formatter:off
		List<DatumStreamTimeRangeValidation> validations = List.of(
					  val("2025-01-01T00:00", "2025-01-02T01:00") // day
					, valHour("2025-01-01T09:00")
					, valHour("2025-01-01T07:00")
					, valHour("2025-01-01T08:00")
					, valHour("2025-01-01T10:00")
				);
		// @formatter:on

		// WHEN
		SortedSet<LocalDateTimeRange> result = DatumStreamTimeRangeValidation.uniqueHourTimeRanges(validations);

		// THEN
		// @formatter:off
		then(result)
			.as("Adjacent hours merged with gap filled")
			.containsExactly(
					  new LocalDateTimeRange(validations.get(2).range().start(), validations.get(4).range().end())
			)
			;
		// @formatter:off
	}

}
