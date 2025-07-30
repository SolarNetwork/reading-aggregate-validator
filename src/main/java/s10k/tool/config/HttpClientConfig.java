package s10k.tool.config;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.core5.util.Timeout;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;

import s10k.tool.support.HttpClientSettings;

/**
 * HTTP client configuration.
 *
 * @author matt
 * @version 1.2
 */
@Configuration(proxyBeanMethods = false)
public class HttpClientConfig {

	@Bean
	@ConfigurationProperties(prefix = "app.http.client.settings")
	public HttpClientSettings httpClientSettings() {
		return new HttpClientSettings();
	}

	@Bean
	public RequestConfig httpRequestConfig(HttpClientSettings settings) {
		RequestConfig config = RequestConfig.custom()
				.setConnectionRequestTimeout(Timeout.of(settings.getConnectionRequestTimeout()))
				.setConnectionKeepAlive(Timeout.of(settings.getConnectionKeepAlive())).build();
		return config;
	}

	@ConfigurationProperties(prefix = "app.http.client.connections")
	@Bean
	public PoolingHttpClientConnectionManager poolingConnectionManager(HttpClientSettings settings) {
		ConnectionConfig config = ConnectionConfig.custom()
				.setConnectTimeout(Timeout.of(settings.getConnectionKeepAlive()))
				.setTimeToLive(Timeout.of(settings.getConnectionTimeToLive()))
				.setSocketTimeout(Timeout.of(settings.getSocketTimeout()))
				.setValidateAfterInactivity(Timeout.of(settings.getConnectionValidateAfterInactivity())).build();
		var poolingConnectionManager = new PoolingHttpClientConnectionManager();
		poolingConnectionManager.setDefaultConnectionConfig(config);
		return poolingConnectionManager;
	}

	@Bean
	public CloseableHttpClient httpClient(HttpClientConnectionManager connectionManager, RequestConfig requestConfig) {
		// @formatter:off
        return HttpClients.custom()
        		.disableCookieManagement()
        		.setDefaultRequestConfig(requestConfig)
        		.useSystemProperties()
                .setConnectionManager(connectionManager)
                .build();
        // @formatter:on
	}

	@Bean
	public HttpComponentsClientHttpRequestFactory clientHttpRequestFactory(CloseableHttpClient httpClient) {
		return new HttpComponentsClientHttpRequestFactory(httpClient);
	}

}
