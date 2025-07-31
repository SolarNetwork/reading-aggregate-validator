package s10k.tool.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.solarnetwork.codec.JsonUtils;

/**
 * JSON configuration.
 */
@Configuration(proxyBeanMethods = false)
public class JsonConfig {

	@Bean
	@Primary
	public ObjectMapper objectMapper() {
		return JsonUtils.newDatumObjectMapper();
	}

	@Bean
	public MappingJackson2HttpMessageConverter objectMapperConverter(ObjectMapper mapper) {
		return new MappingJackson2HttpMessageConverter(mapper);
	}

}
