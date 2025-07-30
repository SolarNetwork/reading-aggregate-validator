package s10k.tool.domain;

import java.math.BigDecimal;

/**
 * A property value comparison.
 */
public record PropertyValueComparison(BigDecimal expected, BigDecimal actual) {

	/**
	 * Get the expected value as a string.
	 * 
	 * @return the string value, never {@code null}
	 */
	public String expectedValue() {
		return (expected != null ? expected.toPlainString() : "");
	}

	/**
	 * Get the actual value as a string.
	 * 
	 * @return the string value, never {@code null}
	 */
	public String actualValue() {
		return (actual != null ? actual.toPlainString() : "");
	}

	/**
	 * Get the difference value as a string {@code (actual - expected)}.
	 * 
	 * @return the string difference value, never {@code null}
	 */
	public String differenceValue() {
		BigDecimal act = (actual != null ? actual : BigDecimal.ZERO);
		BigDecimal exp = (expected != null ? expected : BigDecimal.ZERO);
		return act.subtract(exp).toPlainString();
	}

}
