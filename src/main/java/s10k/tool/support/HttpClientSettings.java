package s10k.tool.support;

import java.time.Duration;

/**
 * Settings for the HTTP client.
 */
public class HttpClientSettings {

	/** The {@code connectTimeout} property default value. */
	public static final long DEFAULT_CONNECT_TIMEOUT_SECS = 10L;

	/** The {@code connectionKeepAlive} property default value. */
	public static final long DEFAULT_CONNECTION_KEEP_ALIVE_SECS = 60L;

	/** The {@code connectionRequestTimeout} property default value. */
	public static final long DEFAULT_CONNECTION_REQUEST_TIMEOUT_SECS = 15L;

	/** The {@code connectionTimeToLive} property default value. */
	public static final long DEFAULT_CONNECTION_TTL_SECS = 60L;

	/** The {@code connectionValidateAfterInactivity} property default value. */
	public static final long DEFAULT_CONNECTION_VALIDATE_AFTER_INACTIVITY_SECS = 20L;

	/** The {@code socketTimeout} property default value. */
	public static final long DEFAULT_SOCKET_TIMEOUT_SECONDS = 20L;

	private Duration connectTimeout = Duration.ofSeconds(DEFAULT_CONNECT_TIMEOUT_SECS);
	private Duration connectionKeepAlive = Duration.ofSeconds(DEFAULT_CONNECTION_KEEP_ALIVE_SECS);
	private Duration connectionRequestTimeout = Duration.ofSeconds(DEFAULT_CONNECTION_REQUEST_TIMEOUT_SECS);
	private Duration connectionTimeToLive = Duration.ofSeconds(DEFAULT_CONNECTION_TTL_SECS);
	private Duration connectionValidateAfterInactivity = Duration
			.ofSeconds(DEFAULT_CONNECTION_VALIDATE_AFTER_INACTIVITY_SECS);
	private Duration socketTimeout = Duration.ofSeconds(DEFAULT_SOCKET_TIMEOUT_SECONDS);

	/**
	 * Get the connection timeout.
	 *
	 * @return the timeout; defaults to {@link #DEFAULT_CONNECT_TIMEOUT_SECS}
	 */
	public Duration getConnectTimeout() {
		return connectTimeout;
	}

	/**
	 * Set the connection timeout.
	 *
	 * @param connectTimeout the timeout to set
	 */
	public void setConnectTimeout(Duration connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	/**
	 * Get the connection keep-alive default timeout.
	 * 
	 * @return the timeout; defaults to {@link #DEFAULT_CONNECTION_KEEP_ALIVE_SECS}
	 */
	public Duration getConnectionKeepAlive() {
		return connectionKeepAlive;
	}

	/**
	 * Set the connection keep-alive default timeout.
	 * 
	 * @param connectionKeepAlive the timeout to set
	 */
	public void setConnectionKeepAlive(Duration connectionKeepAlive) {
		this.connectionKeepAlive = connectionKeepAlive;
	}

	/**
	 * Get the connection pool borrow timeout.
	 *
	 * @return the timeout; defaults to
	 *         {@link #DEFAULT_CONNECTION_REQUEST_TIMEOUT_SECS}
	 */
	public Duration getConnectionRequestTimeout() {
		return connectionRequestTimeout;
	}

	/**
	 * Set the connection pool borrow timeout.
	 *
	 * @param connectionRequestTimeout the connectionRequestTimeout to set
	 */
	public void setConnectionRequestTimeout(Duration connectionRequestTimeout) {
		this.connectionRequestTimeout = connectionRequestTimeout;
	}

	/**
	 * Get the connection time to live timeout.
	 * 
	 * @return the timeout; defaults to {@link #DEFAULT_CONNECTION_TTL_SECS}
	 */
	public Duration getConnectionTimeToLive() {
		return connectionTimeToLive;
	}

	/**
	 * Set the connection time to live timeout.
	 * 
	 * @param connectionTimeToLive the timeout to set
	 */
	public void setConnectionTimeToLive(Duration connectionTimeToLive) {
		this.connectionTimeToLive = connectionTimeToLive;
	}

	/**
	 * Get the connection validate-after-inactivity timeout.
	 * 
	 * @return the timeout; deafults to
	 *         {@link #DEFAULT_CONNECTION_VALIDATE_AFTER_INACTIVITY_SECS}
	 */
	public Duration getConnectionValidateAfterInactivity() {
		return connectionValidateAfterInactivity;
	}

	/**
	 * Set the connection validate-after-inactivity timeout.
	 * 
	 * @param connectionValidateAfterInactivity the timeout to set
	 */
	public void setConnectionValidateAfterInactivity(Duration connectionValidateAfterInactivity) {
		this.connectionValidateAfterInactivity = connectionValidateAfterInactivity;
	}

	/**
	 * Get the socket timeout.
	 *
	 * @return the timeout; defaults to {@link #DEFAULT_SOCKET_TIMEOUT_SECONDS}
	 */
	public Duration getSocketTimeout() {
		return socketTimeout;
	}

	/**
	 * Set the socket timeout.
	 *
	 * @param socketTimeout the timeout to set
	 */
	public void setSocketTimeout(Duration socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

}
