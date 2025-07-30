package s10k.tool.domain;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * A datum stream time range.
 */
public record DatumStreamTimeRange(NodeAndSource nodeAndSource, ZoneId zone, LocalDateTime startDate,
		LocalDateTime endDate) {

}
