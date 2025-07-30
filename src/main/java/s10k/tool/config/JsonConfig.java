package s10k.tool.config;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

import net.solarnetwork.codec.JsonDateUtils;
import net.solarnetwork.codec.JsonUtils;

/**
 * JSON configuration.
 */
@Configuration(proxyBeanMethods = false)
public class JsonConfig {

	// to work with native image, directly load these modules unlike JsonUtils which
	// uses reflection
	private static final Module JAVA_TIME_MODULE;
	static {
		// replace default date+time serializers with ones that support spaces
		SimpleModule m = new SimpleModule("SolarNetwork Time");
		m.addSerializer(Instant.class, JsonDateUtils.InstantSerializer.INSTANCE);
		m.addSerializer(ZonedDateTime.class, JsonDateUtils.ZonedDateTimeSerializer.INSTANCE);
		m.addSerializer(LocalDateTime.class, JsonDateUtils.LocalDateTimeSerializer.INSTANCE);
		m.addDeserializer(Instant.class, JsonDateUtils.InstantDeserializer.INSTANCE);
		m.addDeserializer(ZonedDateTime.class, JsonDateUtils.ZonedDateTimeDeserializer.INSTANCE);
		m.addDeserializer(LocalDateTime.class, JsonDateUtils.LocalDateTimeDeserializer.INSTANCE);
		JAVA_TIME_MODULE = m;
	}

	@Bean
	@Primary
	public ObjectMapper objectMapper() {
		return JsonUtils.createObjectMapper(null, JAVA_TIME_MODULE, JsonUtils.DATUM_MODULE);
	}

	@Bean
	public MappingJackson2HttpMessageConverter objectMapperConverter(ObjectMapper mapper) {
		return new MappingJackson2HttpMessageConverter(mapper);
	}

}
