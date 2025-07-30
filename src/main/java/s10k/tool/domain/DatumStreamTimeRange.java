package s10k.tool.domain;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * A datum stream time range.
 */
public record DatumStreamTimeRange(NodeAndSource nodeAndSource, ZoneId zone, LocalDateTimeRange timeRange) {

	/**
	 * Get the start date.
	 * 
	 * @return the start date
	 */
	public LocalDateTime start() {
		return timeRange.start();
	}

	/**
	 * Get the end date.
	 * 
	 * @return the end date
	 */
	public LocalDateTime end() {
		return timeRange.end();
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of hours
	 */
	public long hourCount() {
		return timeRange.hourCount();
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of months
	 */
	public long monthCount() {
		return timeRange.monthCount();
	}

	/**
	 * Get a new time range starting at this range's start and ending a given hours
	 * later.
	 * 
	 * @param hourCount the count of hours to end at
	 * @return the new time range
	 */
	public DatumStreamTimeRange startingHoursRange(final long hourCount) {
		return new DatumStreamTimeRange(nodeAndSource, zone,
				new LocalDateTimeRange(start(), start().plusHours(hourCount)));
	}

	/**
	 * Get a new time range starting after this range's start plus a given hours
	 * later and ending at this range's end.
	 * 
	 * @param hourCount the count of hours after {@code startDate} to start at
	 * @return the new time range
	 */
	public DatumStreamTimeRange endingHoursRange(final long hourCount) {
		return new DatumStreamTimeRange(nodeAndSource, zone,
				new LocalDateTimeRange(start().plusHours(hourCount), end()));
	}

}
