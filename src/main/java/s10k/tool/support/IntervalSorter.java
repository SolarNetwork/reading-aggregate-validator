package s10k.tool.support;

import java.util.Comparator;

import org.threeten.extra.Interval;

/**
 * Sort intervals by start date, end date.
 */
public class IntervalSorter implements Comparator<Interval> {

	/** A default instance. */
	public static final Comparator<Interval> INSTANCE = new IntervalSorter();

	@Override
	public int compare(Interval o1, Interval o2) {
		int result = o1.getStart().compareTo(o2.getStart());
		if (result == 0) {
			result = o1.getEnd().compareTo(o2.getEnd());
		}
		return result;
	}

}
