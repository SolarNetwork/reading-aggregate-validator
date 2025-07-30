package s10k.tool.domain;

import java.math.BigDecimal;

/**
 * A property value comparison.
 */
public record PropertyValueComparison(BigDecimal expected, BigDecimal actual) {

}
