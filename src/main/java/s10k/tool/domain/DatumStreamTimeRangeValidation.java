package s10k.tool.domain;

import java.util.Map;

/**
 * A datum stream time range with associated validation information.
 */
public record DatumStreamTimeRangeValidation(DatumStreamTimeRange range, DatumStreamValidationInfo validationInfo,
		Map<String, PropertyValueComparison> differences) {

}
