package s10k.tool.validation;

import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.http.MediaType;
import org.springframework.http.client.BufferingClientHttpRequestFactory;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;

import net.solarnetwork.domain.datum.Datum;
import net.solarnetwork.security.Snws2AuthorizationBuilder;
import net.solarnetwork.web.jakarta.support.AuthorizationV2RequestInterceptor;
import net.solarnetwork.web.jakarta.support.LoggingHttpRequestInterceptor;
import net.solarnetwork.web.jakarta.support.StaticAuthorizationCredentialsProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;

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

	private final ClientHttpRequestFactory reqFactory;

	/**
	 * Constructor.
	 * 
	 * @param reqFactory the HTTP request factory to use
	 */
	public ReadingAggregateValidator(ClientHttpRequestFactory reqFactory) {
		super();
		this.reqFactory = reqFactory;
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
			restClient = RestClient.builder(template).baseUrl(DEFAULT_BASE_URL).build();
		} else {
			RestTemplate template = new RestTemplate(reqFactory);
			template.setInterceptors(List.of(new AuthorizationV2RequestInterceptor(credProvider)));
			restClient = RestClient.builder(template).baseUrl(DEFAULT_BASE_URL).build();
		}

		// get listing matching nodes + sources
		final List<NodeAndSource> streams;
		try {
			streams = nodesAndSources(restClient);
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

		List<Future<Collection<InvalidStreamRange>>> taskResults = new ArrayList<>();

		try (ExecutorService threadPool = (threadCount > 1 ? Executors.newFixedThreadPool(threadCount)
				: Executors.newSingleThreadExecutor())) {
			for (Long nodeId : nodeIds) {
				for (String sourceId : sourceIds) {
					taskResults.add(threadPool.submit(new StreamValidator(nodeId, sourceId)));
				}
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

	private List<NodeAndSource> nodesAndSources(RestClient restClient) {
		// @formatter:off
		JsonNode nodesAndSources = restClient.get()
			.uri(b -> {
				b.path("/solarquery/api/v1/sec/nodes/sources");
				b.queryParam("nodeIds", stream(nodeIds).map(Object::toString).collect(joining(",")));
				b.queryParam("sourceIds", stream(sourceIds).collect(joining(",")));
				b.queryParam("accumulatingPropertyNames", stream(properties).collect(joining(",")));
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

	private record NodeAndSource(Long nodeId, String sourceId) {

		private boolean isValid() {
			return nodeId != null && nodeId.longValue() != 0 && sourceId != null && !sourceId.isBlank();
		}

	}

	private final class StreamValidator implements Callable<Collection<InvalidStreamRange>> {
		private final Long nodeId;
		private final String sourceId;

		private final List<InvalidStreamRange> results = new ArrayList<>();

		private StreamValidator(Long nodeId, String sourceId) {
			super();
			this.nodeId = nodeId;
			this.sourceId = sourceId;
		}

		@Override
		public Collection<InvalidStreamRange> call() throws Exception {
			if (verbose) {
				// @formatter:off
				System.out.print(Ansi.AUTO.string("""
						Validation complete for node @|yellow %d|@ source [@|yellow %s|@]: @|%s %d|@ problems found.
						""".formatted(nodeId, sourceId, results.isEmpty() ? "green" : "red", results.size())
						));
				// @formatter:on
			}
			return results;
		}

	}

	private record ValidationInfo(Datum expected, Datum actual) {

	}

	private record InvalidStreamRange(Long nodeId, String sourceId, LocalDateTime start, LocalDateTime end,
			ValidationInfo info) {

	}

}
