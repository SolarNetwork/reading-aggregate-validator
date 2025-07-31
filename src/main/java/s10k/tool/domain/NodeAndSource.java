package s10k.tool.domain;

/**
 * A node and source, uniquely identifying a datum stream.
 * 
 * @param nodeId   the node ID
 * @param sourceId the source ID
 */
public record NodeAndSource(Long nodeId, String sourceId) {

	/**
	 * Test if the node and source are both populated.
	 * 
	 * @return {@code true} if both {@code nodeId} and {@code sourceId} are not
	 *         empty
	 */
	public boolean isValid() {
		return nodeId != null && nodeId.longValue() != 0 && sourceId != null && !sourceId.isBlank();
	}

}
