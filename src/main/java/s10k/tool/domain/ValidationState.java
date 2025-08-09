package s10k.tool.domain;

/**
 * Enumeration of validation execution state.
 */
public enum ValidationState {

	/** The validation was not performed. */
	NotPerformed,

	/** The validation is underway. */
	Processing,

	/** The validation completely finished. */
	Completed,

	/** The validation was stopped before finishing. */
	Incomplete,

	;

}
