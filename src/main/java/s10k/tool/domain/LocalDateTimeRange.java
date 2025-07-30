/**
 * 
 */
package s10k.tool.domain;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * A local date time range.
 */
public record LocalDateTimeRange(LocalDateTime start, LocalDateTime end) implements Comparable<LocalDateTimeRange> {

	@Override
	public int compareTo(LocalDateTimeRange o) {
		int result = start.compareTo(o.start);
		if (result == 0) {
			result = end.compareTo(o.end);
		}
		return result;
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of hours
	 */
	public long hourCount() {
		return start.until(end, ChronoUnit.HOURS);
	}

	/**
	 * Get the number of hours between the start and end dates.
	 * 
	 * @return the number of months
	 */
	public long monthCount() {
		return start.until(end, ChronoUnit.MONTHS);
	}

}
