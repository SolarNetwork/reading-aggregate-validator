package s10k.tool.support;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static net.solarnetwork.util.DateUtils.ISO_DATE_TIME_ALT_UTC;

import java.math.BigDecimal;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;
import org.threeten.extra.Interval;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.solarnetwork.codec.JsonUtils;
import net.solarnetwork.domain.datum.AggregateStreamDatum;
import net.solarnetwork.domain.datum.Aggregation;
import net.solarnetwork.domain.datum.Datum;
import net.solarnetwork.domain.datum.DatumPropertiesStatistics;
import net.solarnetwork.domain.datum.DatumSamples;
import net.solarnetwork.domain.datum.DatumSamplesType;
import net.solarnetwork.domain.datum.GeneralDatum;
import net.solarnetwork.domain.datum.ObjectDatumStreamDataSet;
import net.solarnetwork.domain.datum.ObjectDatumStreamMetadata;
import net.solarnetwork.domain.datum.StreamDatum;
import net.solarnetwork.web.jakarta.security.AuthorizationCredentialsProvider;
import net.solarnetwork.web.jakarta.support.AuthorizationV2RequestInterceptor;
import net.solarnetwork.web.jakarta.support.LoggingHttpRequestInterceptor;
import s10k.tool.domain.DatumStreamTimeRange;
import s10k.tool.domain.NodeAndSource;
import s10k.tool.domain.PropertyValueComparison;
import s10k.tool.domain.TimeRangeValidationDifference;

/**
 * Helper utilities for REST operations.
 */
public final class RestUtils {

	/** The default base URL to the SolarNetwork API. */
	public static final String DEFAULT_SOLARNETWORK_BASE_URL = "https://data.solarnetwork.net";

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
	 * Create a new {@link RestClient} instance.
	 * 
	 * <p>
	 * The client will automatically add a SolarNetwork API authorization header to
	 * each request.
	 * </p>
	 * 
	 * @param reqFactory   the request factory
	 * @param credProvider the SolarNetwork API credentials provider
	 * @param objectMapper the object mapper
	 * @param baseUrl      the base URL
	 * @param traceHttp    {@code true} to enable HTTP trace logging
	 * @return the client
	 */
	public static RestClient createSolarNetworkRestClient(ClientHttpRequestFactory reqFactory,
			AuthorizationCredentialsProvider credProvider, ObjectMapper objectMapper, String baseUrl,
			boolean traceHttp) {
		if (traceHttp) {
			RestTemplate template = new RestTemplate(new BufferingClientHttpRequestFactory(reqFactory));
			template.setInterceptors(
					List.of(new AuthorizationV2RequestInterceptor(credProvider), new LoggingHttpRequestInterceptor()));
			RestUtils.setObjectMapper(template, objectMapper);
			return RestClient.builder(template).baseUrl(baseUrl).build();
		}
		RestTemplate template = new RestTemplate(reqFactory);
		template.setInterceptors(List.of(new AuthorizationV2RequestInterceptor(credProvider)));
		RestUtils.setObjectMapper(template, objectMapper);
		return RestClient.builder(template).baseUrl(baseUrl).build();
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
	 * @throws IllegalArgumentException if both {@code nodeIds} and
	 *                                  {@code sourceIds} are empty
	 * @throws RestClientException      if the request fails
	 */
	public static List<NodeAndSource> nodesAndSources(RestClient restClient, Long[] nodeIds, String[] sourceIds,
			String[] accumulatingProperties) {
		if ((nodeIds == null || nodeIds.length < 1) && (sourceIds == null || sourceIds.length < 1)) {
			throw new IllegalArgumentException("Either node IDs or source IDs (or both) must be provided.");
		}
		// @formatter:off
		JsonNode nodesAndSources = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/datum/stream/meta/node/ids");
				if(nodeIds != null && nodeIds.length > 0) {
					b.queryParam("nodeIds", stream(nodeIds).map(Object::toString).collect(joining(",")));
				}
				if(sourceIds != null && sourceIds.length > 0) {
					b.queryParam("sourceIds", stream(sourceIds).collect(joining(",")));
				}
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
			var nodeSource = new NodeAndSource(tuple.path("objectId").longValue(), tuple.path("sourceId").textValue());
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
	 * @param minDate       an optional minimum allowed starting date value
	 * @param maxDate       an optional maximum allowed end date value
	 * @return the range, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static DatumStreamTimeRange datumStreamTimeRange(RestClient restClient, NodeAndSource nodeAndSource,
			Instant minDate, Instant maxDate) {
		// @formatter:off
		JsonNode range = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/range/interval")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					;
				if (minDate != null) {
					b.queryParam("startDate", minDate.atOffset(UTC).toLocalDateTime());
				}
				if (maxDate != null) {
					b.queryParam("endDate", maxDate.atOffset(UTC).toLocalDateTime());
				}
				return b.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(JsonNode.class)
			.path("data")
			;		
		// @formatter:on
		if (!(range.hasNonNull("startDateMillis") && range.hasNonNull("endDateMillis"))) {
			return null;
		}
		ZoneId zone = (range.hasNonNull("timeZone") ? ZoneId.of(range.get("timeZone").textValue()) : ZoneOffset.UTC);
		Instant startDate = Instant.ofEpochMilli(JsonUtils.parseLongAttribute(range, "startDateMillis"));
		Instant endDate = Instant.ofEpochMilli(JsonUtils.parseLongAttribute(range, "endDateMillis"));

		// round to whole days
		startDate = startDate.atZone(zone).truncatedTo(DAYS).toInstant();
		endDate = endDate.atZone(zone).truncatedTo(DAYS).plusDays(1).toInstant();

		return new DatumStreamTimeRange(nodeAndSource, zone, Interval.of(startDate, endDate));
	}

	private static final ParameterizedTypeReference<ObjectDatumStreamDataSet<StreamDatum>> STREAM_DATUM_SET_TYPEREF = new ParameterizedTypeReference<ObjectDatumStreamDataSet<StreamDatum>>() {
	};

	private static final ParameterizedTypeReference<ObjectDatumStreamDataSet<AggregateStreamDatum>> STREAM_AGG_DATUM_SET_TYPEREF = new ParameterizedTypeReference<ObjectDatumStreamDataSet<AggregateStreamDatum>>() {
	};

	private static Datum firstDatum(ObjectDatumStreamDataSet<AggregateStreamDatum> results,
			String[] accumulatingProperties) {
		for (AggregateStreamDatum d : results) {
			ObjectDatumStreamMetadata streamMeta = results.metadataForStreamId(d.getStreamId());
			return generalDatum(streamMeta, d, accumulatingProperties);
		}
		return null;
	}

	private static NavigableMap<Instant, Datum> allDatum(ObjectDatumStreamDataSet<? extends StreamDatum> results,
			String[] accumulatingProperties) {
		var result = new TreeMap<Instant, Datum>();
		for (StreamDatum d : results) {
			ObjectDatumStreamMetadata streamMeta = results.metadataForStreamId(d.getStreamId());
			Datum datum = generalDatum(streamMeta, d, accumulatingProperties);
			result.put(d.getTimestamp(), datum);
		}
		return result;
	}

	private static GeneralDatum generalDatum(ObjectDatumStreamMetadata streamMeta, StreamDatum d,
			String[] accumulatingProperties) {
		assert streamMeta != null && d != null && accumulatingProperties != null;
		final DatumSamples samples = new DatumSamples();
		final DatumPropertiesStatistics stats = (d instanceof AggregateStreamDatum agg ? agg.getStatistics() : null);
		for (String propName : accumulatingProperties) {
			final int propIdx = streamMeta.propertyIndex(DatumSamplesType.Accumulating, propName);
			if (propIdx >= 0) {
				BigDecimal propVal = (stats != null ? stats.getAccumulatingDifference(propIdx)
						: d.getProperties().accumulatingValue(propIdx));
				samples.putAccumulatingSampleValue(propName, propVal);
			}
		}
		return GeneralDatum.nodeDatum(streamMeta.getObjectId(), streamMeta.getSourceId(), d.getTimestamp(), samples);
	}

	/**
	 * Query for "raw" reading datum.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeAndSource          the datum stream identifier
	 * @param startDate              the start date
	 * @param endDate                the end date
	 * @param limit                  a maximum number of datum to return
	 * @param accumulatingProperties the accumulating properties to extract
	 * @return the datum, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static NavigableMap<Instant, Datum> datum(RestClient restClient, NodeAndSource nodeAndSource,
			Instant startDate, Instant endDate, Integer limit, String[] accumulatingProperties) {
		// @formatter:off
		ObjectDatumStreamDataSet<StreamDatum> results = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/datum/stream/datum")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("startDate", startDate.atOffset(UTC).toLocalDateTime())
					.queryParam("endDate", endDate.atOffset(UTC).toLocalDateTime())
					;
				if (limit != null) {
					b.queryParam("max", limit);
				}
				return b.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(STREAM_DATUM_SET_TYPEREF)
			;
		// @formatter:on
		return allDatum(results, accumulatingProperties);
	}

	/**
	 * Query for a {@code Difference} reading datum.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeAndSource          the datum stream identifier
	 * @param startDate              the start date
	 * @param endDate                the end date
	 * @param accumulatingProperties the accumulating properties to extract
	 * @return the range, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static Datum readingDifference(RestClient restClient, NodeAndSource nodeAndSource, Instant startDate,
			Instant endDate, String[] accumulatingProperties) {
		// @formatter:off
		ObjectDatumStreamDataSet<AggregateStreamDatum> results = restClient.get()
			.uri(b -> {
				return b.path("/solarquery/api/v1/sec/datum/stream/reading")
					.queryParam("readingType", "Difference")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("startDate", startDate.atOffset(UTC).toLocalDateTime())
					.queryParam("endDate", endDate.atOffset(UTC).toLocalDateTime())
					.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(STREAM_AGG_DATUM_SET_TYPEREF)
			;
		// @formatter:on
		return firstDatum(results, accumulatingProperties);
	}

	/**
	 * Query for a {@code Difference} rollup datum, or hourly aggregate datum if the
	 * time range is exactly one hour.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeAndSource          the datum stream identifier
	 * @param zone                   the time zone
	 * @param startDate              the start date
	 * @param endDate                the end date
	 * @param accumulatingProperties the accumulating properties to extract
	 * @param aggregation            the aggregation
	 * @param partialAggregation     an optional partial aggregation
	 * @return the range, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static Datum readingDifferenceRollup(RestClient restClient, NodeAndSource nodeAndSource, ZoneId zone,
			Instant startDate, Instant endDate, String[] accumulatingProperties, Aggregation aggregation,
			Aggregation partialAggregation) {
		// @formatter:off
		ObjectDatumStreamDataSet<AggregateStreamDatum> results = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/datum/stream/reading")
					.queryParam("readingType", "Difference")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("localStartDate", startDate.atZone(zone).toLocalDate())
					.queryParam("localEndDate", endDate.atZone(zone).toLocalDate())
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
			.body(STREAM_AGG_DATUM_SET_TYPEREF)
			;
		// @formatter:on
		return firstDatum(results, accumulatingProperties);
	}

	/**
	 * Query for aggregate datum for a date range.
	 * 
	 * @param restClient             the REST client to use
	 * @param nodeAndSource          the datum stream identifier
	 * @param startDate              the start date
	 * @param endDate                the end date
	 * @param accumulatingProperties the accumulating properties to extract
	 * @param aggregation            the aggregation
	 * @return the datum, or {@code null} if not available
	 * @throws RestClientException if the request fails
	 */
	public static NavigableMap<Instant, Datum> readingDifferenceAggregates(RestClient restClient,
			NodeAndSource nodeAndSource, Instant startDate, Instant endDate, String[] accumulatingProperties,
			Aggregation aggregation) {
		// @formatter:off
		ObjectDatumStreamDataSet<AggregateStreamDatum> results = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/datum/stream/reading")
					.queryParam("readingType", "Difference")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("startDate", startDate.atOffset(UTC).toLocalDateTime())
					.queryParam("endDate", endDate.atOffset(UTC).toLocalDateTime())
					.queryParam("aggregation", aggregation)
					;
				return b.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(STREAM_AGG_DATUM_SET_TYPEREF)
			;
		// @formatter:on
		return allDatum(results, accumulatingProperties);
	}

	/**
	 * Create a URL suitable for marking a datum stream time range as "stale".
	 * 
	 * @param nodeAndSource  the stream identity
	 * @param staleTimeRange the time range
	 * @return the URL
	 */
	public static URI markStaleUri(NodeAndSource nodeAndSource, Interval staleTimeRange) {
		// @formatter:off
		return UriComponentsBuilder.newInstance()
			.path("/solaruser/api/v1/sec/datum/maint/agg/stale")
			.queryParam("nodeId", nodeAndSource.nodeId())
			.queryParam("sourceId", nodeAndSource.sourceId())
			.queryParam("startDate", staleTimeRange.getStart().atOffset(UTC).toLocalDateTime())
			.queryParam("endDate", staleTimeRange.getEnd().atOffset(UTC).toLocalDateTime())
			.build()
			.toUri()
			;
		// @formatter:on
	}

	/**
	 * Mark a datum stream time range as "stale".
	 * 
	 * @param restClient     the REST client to use
	 * @param nodeAndSource  the stream identity
	 * @param staleTimeRange the time range
	 * @return {@code true} if successful
	 * @throws RestClientException if the request fails
	 */
	public static boolean markStale(RestClient restClient, NodeAndSource nodeAndSource, Interval staleTimeRange) {
		var postBody = new LinkedMultiValueMap<String, Object>(4);
		postBody.set("nodeId", nodeAndSource.nodeId());
		postBody.set("sourceId", nodeAndSource.sourceId());
		postBody.set("startDate", staleTimeRange.getStart().atOffset(UTC).toLocalDateTime().toString());
		postBody.set("endDate", staleTimeRange.getEnd().atOffset(UTC).toLocalDateTime().toString());
		// @formatter:off
		JsonNode success = restClient.post()
			.uri("/solaruser/api/v1/sec/datum/maint/agg/stale")
			.contentType(MediaType.APPLICATION_FORM_URLENCODED)
			.body(postBody)
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(JsonNode.class)
			.path("success")
			;		
		// @formatter:on
		return success.booleanValue();
	}

	/**
	 * Test if at least one datum auxiliary "reset" record is found within a given
	 * date range.
	 * 
	 * @param restClient    the REST client to use
	 * @param nodeAndSource the stream identity
	 * @param timeRange     the time range
	 * @return {@code true} if one or more reset records are found
	 * @throws RestClientException if the request fails
	 */
	public static boolean hasDatumAuxiliaryResetRecord(RestClient restClient, NodeAndSource nodeAndSource,
			Interval timeRange) {
		// @formatter:off
		JsonNode results = restClient.get()
			.uri(b -> {
				b.path("/solaruser/api/v1/sec/datum/auxiliary")
					.queryParam("nodeId", nodeAndSource.nodeId())
					.queryParam("sourceId", nodeAndSource.sourceId())
					.queryParam("startDate", timeRange.getStart().atOffset(UTC).toLocalDateTime())
					.queryParam("endDate", timeRange.getEnd().atOffset(UTC).toLocalDateTime())
					.queryParam("max", 1)
					.queryParam("withoutTotalResultsCount", true)
					;
				return b.build();
			})
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(JsonNode.class)
			.findPath("results")
			;
		// @formatter:on
		return (results != null && !results.findPath("results").isEmpty());
	}

	/**
	 * Save a datum auxiliary reset record.
	 * 
	 * @param restClient    the REST client to use
	 * @param nodeAndSource the stream identity
	 * @param resetDiff     the differences to save in the reset record
	 * @return {@code true} if the reset record is saved successfully
	 * @throws RestClientException if the request fails
	 */
	public static Instant saveDatumAuxiliaryResetRecord(RestClient restClient, NodeAndSource nodeAndSource,
			TimeRangeValidationDifference resetDiff) {
		if (!resetDiff.hasDifferences()) {
			return null;
		}
		// the basic structure looks like
		/*-
			{
			  "created": "2022-07-22 20:12:00Z",
			  "nodeId": "123",
			  "sourceId": "/a/b/d",
			  "type": "Reset",
			  "final": {
			    "a": {
			      "wattHours": 3210
			    }
			  },
			  "start": {
			    "a": {
			      "wattHours": 1230
			    }
			  }
			}
		*/
		Instant resetDate = resetDiff.range().getEnd().minus(1, MINUTES).truncatedTo(MINUTES);
		Map<String, Number> finalProperties = new LinkedHashMap<String, Number>(resetDiff.differences().size());
		Map<String, Number> startProperties = new LinkedHashMap<String, Number>(resetDiff.differences().size());
		for (Entry<String, PropertyValueComparison> entry : resetDiff.differences().entrySet()) {
			// because of the way we called differences() above, the "expected"
			// value is the reading start
			// and the "actual" is the found "next" value
			finalProperties.put(entry.getKey(), entry.getValue().expected());
			startProperties.put(entry.getKey(), entry.getValue().actual());
		}
		// @formatter:off
		Map<String, Object> resetData = Map.of(
				"created", ISO_DATE_TIME_ALT_UTC.format(resetDate),
				"nodeId", nodeAndSource.nodeId(),
				"sourceId", nodeAndSource.sourceId(),
				"type", "Reset",
				"final", Map.of("a", finalProperties),
				"start", Map.of("a", startProperties),
				"notes", "Created by sn-reading-aggregate-validator"
			);
		// @formatter:on

		// @formatter:off
		JsonNode success = restClient.post()
			.uri("/solaruser/api/v1/sec/datum/auxiliary")
			.contentType(MediaType.APPLICATION_JSON)
			.body(resetData)
			.accept(MediaType.APPLICATION_JSON)
			.retrieve()
			.body(JsonNode.class)
			.path("success")
			;		
		// @formatter:on
		if (success.booleanValue()) {
			return resetDate;
		}
		return null;
	}

}
