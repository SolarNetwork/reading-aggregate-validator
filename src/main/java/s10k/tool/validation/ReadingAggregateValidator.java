package s10k.tool.validation;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static net.solarnetwork.util.DateUtils.ISO_DATE_OPT_TIME_ALT_LOCAL;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.solarnetwork.domain.datum.Aggregation;
import net.solarnetwork.domain.datum.Datum;
import net.solarnetwork.security.Snws2AuthorizationBuilder;
import net.solarnetwork.web.jakarta.support.AuthorizationV2RequestInterceptor;
import net.solarnetwork.web.jakarta.support.LoggingHttpRequestInterceptor;
import net.solarnetwork.web.jakarta.support.StaticAuthorizationCredentialsProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import s10k.tool.domain.DatumStreamTimeRange;
import s10k.tool.domain.DatumStreamTimeRangeValidation;
import s10k.tool.domain.DatumStreamValidationInfo;
import s10k.tool.domain.NodeAndSource;
import s10k.tool.domain.PropertyValueComparison;
import s10k.tool.support.RestUtils;

/**
 * Validate SolarNetwork aggregate reading values.
 */
@Component
@Command(name = "validate")
public class ReadingAggregateValidator implements Callable<Integer> {

	private static final String DEFAULT_BASE_URL = "https://data.solarnetwork.net";

	@Option(names = { "-v", "--verbose" }, description = "verbose output")
	boolean verbose;

	@Option(names = { "-n", "--dry-run" }, description = "do not actually submit changes to SolarNetwork")
	boolean dryRun;

	@Option(names = { "-T", "--http-trace" }, description = "trace HTTP exchanges")
	boolean traceHttp;

	@Option(names = { "-j", "--threads" }, description = "number of concurrent threads", defaultValue = "1")
	int threadCount = 1;

	@Option(names = { "-h", "--help" }, usageHelp = true, description = "display this help message")
	boolean usageHelpRequested;

	@Option(names = { "-node",
			"--node-id" }, description = "a node ID to validate", required = true, split = "\\s*,\\s*", splitSynopsisLabel = ",")
	Long[] nodeIds;

	@Option(names = { "-source",
			"--source-id" }, description = "a source ID to validate", required = true, split = "\\s*,\\s*", splitSynopsisLabel = ",")
	String[] sourceIds;

	@Option(names = { "-prop",
			"--property" }, description = "a property name to validate", required = true, split = "\\s*,\\s*", splitSynopsisLabel = ",")
	String[] properties;

	@Option(names = { "-w", "--max-wait" }, description = "maximum time to wait for validation to complete")
	Duration maxWait = Duration.ofMinutes(10L);

	@Option(names = { "-u", "--token" }, description = "the SolarNetwork API token", required = true)
	String tokenId;

	@Option(names = { "-p",
			"--secret" }, description = "the SolarNetwork API token secret", required = true, interactive = true)
	char[] tokenSecret;

	@Option(names = { "-o",
			"--min-days-offset" }, description = "minimum number of days offset from today to disallow validation", defaultValue = "1")
	int minDaysOffsetFromNow = 1;

	private final ClientHttpRequestFactory reqFactory;
	private final ObjectMapper objectMapper;

	/**
	 * Constructor.
	 * 
	 * @param reqFactory   the HTTP request factory to use
	 * @param objectMapper the mapper to use
	 */
	public ReadingAggregateValidator(ClientHttpRequestFactory reqFactory, ObjectMapper objectMapper) {
		super();
		this.reqFactory = reqFactory;
		this.objectMapper = objectMapper;
	}

	@Override
	public Integer call() throws Exception {
		if (verbose) {
			// @formatter:off
			System.out.println(Ansi.AUTO.string("""
					@|bold Nodes:|@   %s
					@|bold Sources:|@ %s
					""".formatted(
							stream(nodeIds).map(v -> "@|yellow %d|@".formatted(v)).collect(joining(", ")),
							stream(sourceIds).map(v -> "@|yellow %s|@".formatted(v)).collect(joining(", "))
					)));
			// @formatter:on
		}

		// compute signing key, then throw out the secret
		final Instant signingDate = Instant.now();
		final var credProvider = new StaticAuthorizationCredentialsProvider(tokenId,
				new Snws2AuthorizationBuilder(tokenId).computeSigningKey(signingDate, String.valueOf(tokenSecret)),
				signingDate);
		Arrays.fill(tokenSecret, ' ');

		final RestClient restClient;
		if (traceHttp) {
			RestTemplate template = new RestTemplate(new BufferingClientHttpRequestFactory(reqFactory));
			template.setInterceptors(
					List.of(new AuthorizationV2RequestInterceptor(credProvider), new LoggingHttpRequestInterceptor()));
			RestUtils.setObjectMapper(template, objectMapper);
			restClient = RestClient.builder(template).baseUrl(DEFAULT_BASE_URL).build();
		} else {
			RestTemplate template = new RestTemplate(reqFactory);
			template.setInterceptors(List.of(new AuthorizationV2RequestInterceptor(credProvider)));
			RestUtils.setObjectMapper(template, objectMapper);
			restClient = RestClient.builder(template).baseUrl(DEFAULT_BASE_URL).build();
		}

		// get listing matching nodes + sources
		final List<NodeAndSource> streams;
		try {
			streams = RestUtils.nodesAndSources(restClient, nodeIds, sourceIds, properties);
		} catch (RestClientResponseException e) {
			// @formatter:off
			System.out.print(Ansi.AUTO.string("""
					@|red Error listing datum streams:|@  HTTP status %s returned.
					""".formatted(e.getStatusCode())));
			// @formatter:on
			return 1;
		}
		if (streams.isEmpty()) {
			// @formatter:off
			System.out.print(Ansi.AUTO.string("""
					No datum streams available for the given node/source/property combinations.
					"""));
			// @formatter:on
			return 1;
		}

		List<Future<Collection<DatumStreamTimeRangeValidation>>> taskResults = new ArrayList<>();

		try (ExecutorService threadPool = (threadCount > 1 ? Executors.newFixedThreadPool(threadCount)
				: Executors.newSingleThreadExecutor())) {
			for (NodeAndSource nodeAndSource : streams) {
				taskResults.add(threadPool.submit(new StreamValidator(restClient, nodeAndSource)));
			}
			threadPool.shutdown();
			boolean finished = threadPool.awaitTermination(maxWait.toSeconds(), TimeUnit.SECONDS);
			if (!finished) {
				System.out.println("Validation tasks did not complete within %ds.".formatted(maxWait.toSeconds()));
				threadPool.shutdownNow();
			}
		}

		return 0;
	}

	private final class StreamValidator implements Callable<Collection<DatumStreamTimeRangeValidation>> {

		private final RestClient restClient;
		private final NodeAndSource nodeAndSource;
		private final String verboseMessagePrefix;

		private final List<DatumStreamTimeRangeValidation> results = new ArrayList<>();

		private StreamValidator(RestClient restClient, NodeAndSource nodeAndSource) {
			super();
			this.restClient = restClient;
			this.nodeAndSource = nodeAndSource;
			this.verboseMessagePrefix = "[@|yellow %6d|@ @|yellow %s|@]".formatted(nodeAndSource.nodeId(),
					nodeAndSource.sourceId());
		}

		@Override
		public Collection<DatumStreamTimeRangeValidation> call() throws Exception {
			if (verbose) {
				System.out.println(Ansi.AUTO.string(verboseMessagePrefix + " Validation starting"));
			}

			final DatumStreamTimeRange range = RestUtils.datumStreamTimeRange(restClient, nodeAndSource,
					Instant.now().minus(minDaysOffsetFromNow, DAYS).truncatedTo(DAYS));
			if (range == null) {
				System.out.println(verboseMessagePrefix + " Time range not available");
				return List.of();
			}
			if (verbose) {
				// @formatter:off
				System.out.print(Ansi.AUTO.string("""
						%s Stream range discovered: %s - %s (%s; %d days)
						""".formatted(
								  verboseMessagePrefix
								, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.startDate())
								, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.endDate())
								, range.zone().getId()
								, DAYS.between(range.startDate(), range.endDate())
							)
						));
				// @formatter:on
			}

			final Datum expected = RestUtils.readingDifference(restClient, nodeAndSource, range.startDate(),
					range.endDate(), properties);
			final Datum rollup = RestUtils.readingDifferenceRollup(restClient, nodeAndSource, range.startDate(),
					range.endDate(), properties, Aggregation.Month, Aggregation.Day);

			final DatumStreamValidationInfo info = new DatumStreamValidationInfo(expected, rollup, properties);
			final Map<String, PropertyValueComparison> differences = info.differences();
			results.add(new DatumStreamTimeRangeValidation(range, info, differences));

			if (verbose) {
				// @formatter:off
				System.out.print(Ansi.AUTO.string("""
						%s Validation complete: @|%s %d|@ problems found
						""".formatted(
								  verboseMessagePrefix
								, results.isEmpty() ? "green" : "red"
								, results.size()
							)
						));
				// @formatter:on
			}
			return results;
		}

	}

}
