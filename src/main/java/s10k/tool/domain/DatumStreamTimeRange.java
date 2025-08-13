package s10k.tool.domain;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MONTHS;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import org.threeten.extra.Interval;

/**
 * A datum stream time range.
 * 
 * @param nodeAndSource the stream identifier
 * @param zone          the stream time zone
 * @param timeRange     the time range
 */
public record DatumStreamTimeRange(NodeAndSource nodeAndSource, ZoneId zone, Interval timeRange) {

	/**
	 * Get the start date.
	 * 
	 * @return the start date
	 */
	public Instant start() {
		return timeRange.getStart();
	}

	/**
	 * Get the end date.
	 * 
	 * @return the end date
	 */
	public Instant end() {
		return timeRange.getEnd();
	}

	/**
	 * Get the start date.
	 * 
	 * @return the start date
	 */
	public LocalDateTime startLocal() {
		return timeRange.getStart().atZone(zone).toLocalDateTime();
	}

	/**
	 * Get the end date.
	 * 
	 * @return the end date
	 */
	public LocalDateTime endLocal() {
		return timeRange.getEnd().atZone(zone).toLocalDateTime();
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of hours
	 */
	public long hourCount() {
		return start().until(end(), HOURS);
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of hours
	 */
	public long dayCount() {
		return start().atZone(zone).until(end().atZone(zone), DAYS);
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of months
	 */
	public long monthCount() {
		return start().atZone(zone).until(end().atZone(zone), MONTHS);
	}

	/**
	 * Get a new time range starting at this range's start and ending a given days
	 * later.
	 * 
	 * @param dayCount the count of days to end at
	 * @return the new time range
	 */
	public DatumStreamTimeRange startingDaysRange(final long dayCount) {
		return new DatumStreamTimeRange(nodeAndSource, zone,
				Interval.of(start(), start().atZone(zone).plusDays(dayCount).toInstant()));
	}

	/**
	 * Get a new time range starting after this range's start plus a given days
	 * later and ending at this range's end.
	 * 
	 * @param dayCount the count of days after {@code startDate} to start at
	 * @return the new time range
	 */
	public DatumStreamTimeRange endingDaysRange(final long dayCount) {
		return new DatumStreamTimeRange(nodeAndSource, zone,
				Interval.of(start().atZone(zone).plusDays(dayCount).toInstant(), end()));
	}

}
