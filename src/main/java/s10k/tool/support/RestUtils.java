package s10k.tool.support;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static net.solarnetwork.codec.JsonUtils.parseDateAttribute;
import static net.solarnetwork.util.DateUtils.ISO_DATE_OPT_TIME_ALT_LOCAL;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.solarnetwork.domain.datum.Aggregation;
import net.solarnetwork.domain.datum.Datum;
import net.solarnetwork.domain.datum.DatumProperties;
import net.solarnetwork.domain.datum.DatumSamples;
import net.solarnetwork.domain.datum.DatumSamplesType;
import net.solarnetwork.domain.datum.GeneralDatum;
import net.solarnetwork.domain.datum.ObjectDatumStreamDataSet;
import net.solarnetwork.domain.datum.ObjectDatumStreamMetadata;
import net.solarnetwork.domain.datum.StreamDatum;
import s10k.tool.domain.DatumStreamTimeRange;
import s10k.tool.domain.LocalDateTimeRange;
import s10k.tool.domain.NodeAndSource;

/**
 * Helper utilities for REST operations.
 */
public final class RestUtils {

	private RestUtils() {
		// not available
	}

	/**
	 * Set the ObjectMapper used by a {@link RestTemplate}.
	 * 
	 * @param template     the template to adjust
	 * @param objectMapper the object mapper to use
	 */
	public static void setObjectMapper(RestTemplate template, ObjectMapper objectMapper) {
		for (HttpMessageConverter<?> converter : template.getMessageConverters()) {
			if (converter instanceof MappingJackson2HttpMessageConverter c) {
				c.setObjectMapper(objectMapper);
			}
		}
	}

	/**
	 * Query for all available node and source combinations.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeIds                the node IDs to query
	 * @param sourceIds              the source IDs (can include SolarNetwork
	 *                               wildcard patterns)
	 * @param accumulatingProperties the accumulating properties that must be
	 *                               available in the datum streams
	 * @return the nodes and sources
	 * @throws RestClientException if the request fails
	 */
	public static List<NodeAndSource> nodesAndSources(RestClient restClient, Long[] nodeIds, String[] sourceIds,
			String[] accumulatingProperties) {
		// @formatter:off
		JsonNode nodesAndSources = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/nodes/sources");
				b.queryParam("nodeIds", stream(nodeIds).map(Object::toString).collect(joining(",")));
				b.queryParam("sourceIds", stream(sourceIds).collect(joining(",")));
				b.queryParam("accumulatingPropertyNames", stream(accumulatingProperties).collect(joining(",")));
				return b.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(JsonNode.class)
			;		
		// @formatter:on
		List<NodeAndSource> results = new ArrayList<>();
		for (JsonNode tuple : nodesAndSources.findPath("data")) {
			var nodeSource = new NodeAndSource(tuple.path("nodeId").longValue(), tuple.path("sourceId").textValue());
			if (nodeSource.isValid()) {
				results.add(nodeSource);
			}
		}
		return results;
	}

	/**
	 * Query for the available date range for a datum stream, rounded to day
	 * granularity.
	 * 
	 * @param restClient    the REST client to use
	 * @param nodeAndSource the datum stream identifier
	 * @param maxDate       the maximum allowed end date value
	 * @return the range, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static DatumStreamTimeRange datumStreamTimeRange(RestClient restClient, NodeAndSource nodeAndSource,
			Instant maxDate) {
		// @formatter:off
		JsonNode range = restClient.get()
			.uri(b -> {
				return b.path("/solarquery/api/v1/sec/range/interval")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(JsonNode.class)
			.path("data")
			;		
		// @formatter:on
		ZoneId zone = (range.hasNonNull("timeZone") ? ZoneId.of(range.get("timeZone").textValue()) : ZoneOffset.UTC);
		LocalDateTime startDate = parseDateAttribute(range, "startDate", ISO_DATE_OPT_TIME_ALT_LOCAL,
				LocalDateTime::from);
		LocalDateTime endDate = parseDateAttribute(range, "endDate", ISO_DATE_OPT_TIME_ALT_LOCAL, LocalDateTime::from);
		if (startDate == null || endDate == null) {
			return null;
		}

		// round to whole hours
		startDate = startDate.truncatedTo(DAYS);
		endDate = endDate.truncatedTo(DAYS).plusDays(1);

		// enforce max date
		Instant endInstant = endDate.atZone(zone).toInstant();
		if (endInstant.isAfter(maxDate)) {
			endDate = maxDate.atZone(zone).toLocalDateTime().truncatedTo(DAYS);
		}

		return new DatumStreamTimeRange(nodeAndSource, zone, new LocalDateTimeRange(startDate, endDate));
	}

	private static final ParameterizedTypeReference<ObjectDatumStreamDataSet<StreamDatum>> STREAM_DATUM_SET_TYPEREF = new ParameterizedTypeReference<ObjectDatumStreamDataSet<StreamDatum>>() {
	};

	private static Datum firstDatum(ObjectDatumStreamDataSet<StreamDatum> results, String[] accumulatingProperties) {
		for (StreamDatum d : results) {
			ObjectDatumStreamMetadata streamMeta = results.metadataForStreamId(d.getStreamId());
			assert streamMeta != null;
			DatumSamples samples = new DatumSamples();
			DatumProperties props = d.getProperties();
			for (String propName : accumulatingProperties) {
				int propIdx = streamMeta.propertyIndex(DatumSamplesType.Accumulating, propName);
				if (propIdx >= 0) {
					BigDecimal propVal = props.accumulatingValue(propIdx);
					samples.putAccumulatingSampleValue(propName, propVal);
				}
			}
			return GeneralDatum.nodeDatum(streamMeta.getObjectId(), streamMeta.getSourceId(), d.getTimestamp(),
					samples);
		}
		return null;
	}

	/**
	 * Query for the available date range for a datum stream, rounded to hour
	 * granularity.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeAndSource          the datum stream identifier
	 * @param startDate              the start date
	 * @param endDate                the end date
	 * @param accumulatingProperties the accumulating properties to extract
	 * @return the range, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static Datum readingDifference(RestClient restClient, NodeAndSource nodeAndSource, LocalDateTime startDate,
			LocalDateTime endDate, String[] accumulatingProperties) {
		// @formatter:off
		ObjectDatumStreamDataSet<StreamDatum> results = restClient.get()
			.uri(b -> {
				return b.path("/solarquery/api/v1/sec/datum/stream/reading")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("localStartDate", startDate)
					.queryParam("localEndDate", endDate)
					.queryParam("readingType", "Difference")
					.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(STREAM_DATUM_SET_TYPEREF)
			;
		// @formatter:on
		return firstDatum(results, accumulatingProperties);
	}

	/**
	 * Query for the available date range for a datum stream, rounded to hour
	 * granularity.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeAndSource          the datum stream identifier
	 * @param startDate              the start date
	 * @param endDate                the end date
	 * @param accumulatingProperties the accumulating properties to extract
	 * @param aggregation            the aggregation
	 * @param partialAggregation     an optional partial aggregation
	 * @return the range, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static Datum readingDifferenceRollup(RestClient restClient, NodeAndSource nodeAndSource,
			LocalDateTime startDate, LocalDateTime endDate, String[] accumulatingProperties, Aggregation aggregation,
			Aggregation partialAggregation) {
		// @formatter:off
		ObjectDatumStreamDataSet<StreamDatum> results = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/datum/stream/reading")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("localStartDate", startDate)
					.queryParam("localEndDate", endDate)
					.queryParam("readingType", "Difference")
					.queryParam("aggregation", aggregation)
					;
				if (ChronoUnit.HOURS.between(startDate, endDate) > 1) {
					b.queryParam("rollupType", "All");
					if (partialAggregation != null ) {
						b.queryParam("partialAggregation", partialAggregation);
					}
				}
				return b.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(STREAM_DATUM_SET_TYPEREF)
			;
		// @formatter:on
		return firstDatum(results, accumulatingProperties);
	}

}
