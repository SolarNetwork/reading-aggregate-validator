package s10k.tool.validation;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static net.solarnetwork.domain.datum.Aggregation.Day;
import static net.solarnetwork.domain.datum.Aggregation.Hour;
import static net.solarnetwork.util.DateUtils.ISO_DATE_OPT_TIME_ALT_LOCAL;
import static org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE;
import static s10k.tool.domain.DatumStreamTimeRangeValidation.uniqueHourTimeRanges;

import java.io.FileWriter;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.solarnetwork.domain.datum.Aggregation;
import net.solarnetwork.domain.datum.Datum;
import net.solarnetwork.io.NullWriter;
import net.solarnetwork.security.Snws2AuthorizationBuilder;
import net.solarnetwork.web.jakarta.support.StaticAuthorizationCredentialsProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import s10k.tool.domain.DatumStreamTimeRange;
import s10k.tool.domain.DatumStreamTimeRangeValidation;
import s10k.tool.domain.DatumStreamValidationInfo;
import s10k.tool.domain.LocalDateTimeRange;
import s10k.tool.domain.NodeAndSource;
import s10k.tool.domain.PropertyValueComparison;
import s10k.tool.support.RestUtils;

/**
 * Validate SolarNetwork aggregate reading values.
 */
@Component
@Command(name = "validate")
public class ReadingAggregateValidator implements Callable<Integer> {

	private static final long HOURS_PER_DAY = 24L;

	@Option(names = { "-v", "--verbose" }, description = "verbose output")
	boolean verbose;

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

	@Option(names = { "-m", "--mark-stale" }, description = "mark the invalid time ranges as stale in SolarNetwork")
	boolean markStale;

	@Option(names = { "-r", "--report-file" }, description = "path to write CSV report data")
	String reportFileName;

	@Option(names = { "-X",
			"--max-invalid" }, description = "maximum invalid ranges per stream before giving up, or 0 for unlimited")
	int maxStreamInvalid;

	private final ClientHttpRequestFactory reqFactory;
	private final ObjectMapper objectMapper;

	private volatile boolean globalStop;

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

		final RestClient restClient = RestUtils.createSolarNetworkRestClient(reqFactory, credProvider, objectMapper,
				RestUtils.DEFAULT_SOLARNETWORK_BASE_URL, traceHttp);

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
				globalStop = true;
				threadPool.shutdownNow();
			}
		}

		for (Future<Collection<DatumStreamTimeRangeValidation>> taskResult : taskResults) {
			if (!taskResult.isDone()) {
				continue;
			}
			try (ICsvListWriter csv = new CsvListWriter(
					reportFileName != null ? new FileWriter(reportFileName, UTF_8) : new NullWriter(),
					STANDARD_PREFERENCE)) {
				csv.writeHeader("Node", "Source", "Start", "End", "Property", "Expected", "Actual", "Difference");

				Collection<DatumStreamTimeRangeValidation> validations = taskResult.get();

				// generate full list of hours with differences as CSV data
				List<DatumStreamTimeRangeValidation> invalidHours = validations.stream()
						.filter(DatumStreamTimeRangeValidation::isHourRange)
						.sorted((l, r) -> l.range().timeRange().compareTo(r.range().timeRange())).toList();
				if (!invalidHours.isEmpty()) {
					// all results will be for single stream
					final NodeAndSource streamIdent = invalidHours.getFirst().range().nodeAndSource();
					final String streamIdentMessagePrefix = nodeAndSourceMessagePrefix(streamIdent);

					// @formatter:off
					System.out.print(Ansi.AUTO.string("""
							%s @|red %d|@ validation problems found
							""".formatted(
									  streamIdentMessagePrefix
									, invalidHours.size()
								)
							));
					// @formatter:on

					for (DatumStreamTimeRangeValidation invalidHour : invalidHours) {
						boolean repeat = false;
						for (Entry<String, PropertyValueComparison> diffEntry : invalidHour.differences().entrySet()) {
							String[] row = new String[8];
							if (!repeat) {
								row[0] = invalidHour.range().nodeAndSource().nodeId().toString();
								row[1] = invalidHour.range().nodeAndSource().sourceId();
								row[2] = ISO_DATE_OPT_TIME_ALT_LOCAL.format(invalidHour.range().timeRange().start());
								row[3] = ISO_DATE_OPT_TIME_ALT_LOCAL.format(invalidHour.range().timeRange().end());
								repeat = true;
							} else {
								Arrays.fill(row, 0, 4, "");
							}
							row[4] = diffEntry.getKey();
							row[5] = diffEntry.getValue().expectedValue();
							row[6] = diffEntry.getValue().actualValue();
							row[7] = diffEntry.getValue().differenceValue();
							csv.write(row);
						}
					}

					SortedSet<LocalDateTimeRange> staleTimeRanges = uniqueHourTimeRanges(validations);
					if (!staleTimeRanges.isEmpty()) {
						// @formatter:off
						System.out.println(Ansi.AUTO.string("""
								%s @|red %d|@ ranges to mark stale
								""".formatted(
										  streamIdentMessagePrefix
										, staleTimeRanges.size()
									)
								));
						// @formatter:on

						for (LocalDateTimeRange staleTimeRange : staleTimeRanges) {
							URI markStaleUri = RestUtils.markStaleUri(streamIdent, staleTimeRange);
							if (markStale) {
								boolean marked = RestUtils.markStale(restClient, streamIdent, staleTimeRange);
								if (marked) {
									// @formatter:off
									System.out.print(Ansi.AUTO.string("""
											%s Marked range %s - %s as stale (%d hours)
											""".formatted(
													  streamIdentMessagePrefix
													, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.start())
													, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.end())
													, HOURS.between(staleTimeRange.start(), staleTimeRange.end())
												)
											));
									// @formatter:on
								} else {
									// @formatter:off
									System.out.print(Ansi.AUTO.string("""
											%s @|Error|@ marking range %s - %s as stale (%d hours)
											""".formatted(
													  streamIdentMessagePrefix
													, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.start())
													, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.end())
													, HOURS.between(staleTimeRange.start(), staleTimeRange.end())
												)
											));
									// @formatter:on
								}
							} else {
								System.out.println(markStaleUri);
							}
						}
					}
				}
			} catch (ExecutionException e) {
				System.err.print("Validation task failed: " + e.getCause());
			}
		}

		return 0;
	}

	private static String nodeAndSourceMessagePrefix(NodeAndSource nodeAndSource) {
		return "[@|yellow %6d|@ @|yellow %s|@]".formatted(nodeAndSource.nodeId(), nodeAndSource.sourceId());
	}

	private final class StreamValidator implements Callable<Collection<DatumStreamTimeRangeValidation>> {

		private final RestClient restClient;
		private final NodeAndSource nodeAndSource;
		private final String streamMessagePrefix;

		private final List<DatumStreamTimeRangeValidation> results = new ArrayList<>();
		private long invalidHours = 0L;
		private boolean stop;

		private StreamValidator(RestClient restClient, NodeAndSource nodeAndSource) {
			super();
			this.restClient = restClient;
			this.nodeAndSource = nodeAndSource;
			this.streamMessagePrefix = nodeAndSourceMessagePrefix(nodeAndSource);
		}

		@Override
		public Collection<DatumStreamTimeRangeValidation> call() throws Exception {
			if (verbose) {
				System.out.println(Ansi.AUTO.string(streamMessagePrefix + " Validation starting"));
			}

			final DatumStreamTimeRange range = RestUtils.datumStreamTimeRange(restClient, nodeAndSource,
					Instant.now().minus(minDaysOffsetFromNow, DAYS).truncatedTo(DAYS));
			if (range == null) {
				System.out.println(streamMessagePrefix + " Time range not available");
				return List.of();
			}
			if (verbose) {
				// @formatter:off
				System.out.print(Ansi.AUTO.string("""
						%s Stream range discovered: %s - %s (%s; %d days)
						""".formatted(
								  streamMessagePrefix
								, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.start())
								, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.end())
								, range.zone().getId()
								, DAYS.between(range.start(), range.end())
							)
						));
				// @formatter:on
			}

			findDifferences(range);

			if (verbose) {
				System.out.println(Ansi.AUTO.string("%s Validation complete".formatted(streamMessagePrefix)));
			}
			return results;
		}

		private void findDifferences(DatumStreamTimeRange range) {
			if (stop || globalStop) {
				return;
			}
			final long rangeHours = range.hourCount();
			if (rangeHours < 1) {
				return;
			}

			final long rangeMonths = range.monthCount();
			final Aggregation aggregation;
			final Aggregation partialAggregation;
			if (rangeMonths > 1) {
				aggregation = Aggregation.Month;
				partialAggregation = Aggregation.Day;
			} else {
				aggregation = Day;
				partialAggregation = Hour;
			}

			final DatumStreamValidationInfo info = queryDifference(range, aggregation, partialAggregation);
			final Map<String, PropertyValueComparison> differences = info.differences(properties);
			if (!differences.isEmpty()) {
				results.add(new DatumStreamTimeRangeValidation(range, info, differences));

				if (verbose || range.hourCount() > 1) {
					// @formatter:off
					System.out.print(Ansi.AUTO.string("""
							%s Difference discovered in range %s - %s (%s; %d days)
							""".formatted(
									  streamMessagePrefix
									, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.start())
									, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.end())
									, range.zone().getId()
									, DAYS.between(range.start(), range.end())
								)
							));
					// @formatter:on
				}

				if (rangeHours <= HOURS_PER_DAY) {
					// reach final hour-level aggregate comparison so iterate over hours
					for (LocalDateTime hour = range.start(); hour.isBefore(range.end()); hour = hour.plusHours(1)) {
						final DatumStreamTimeRange hourRange = new DatumStreamTimeRange(range.nodeAndSource(),
								range.zone(), new LocalDateTimeRange(hour, hour.plusHours(1)));
						final DatumStreamValidationInfo hourInfo = queryDifference(hourRange, Hour, null);
						final Map<String, PropertyValueComparison> hourDifferences = hourInfo.differences(properties);
						if (!hourDifferences.isEmpty()) {
							results.add(new DatumStreamTimeRangeValidation(hourRange, hourInfo, hourDifferences));
							invalidHours++;
							if (maxStreamInvalid > 0 && !(invalidHours < maxStreamInvalid)) {
								stop = true;
								// @formatter:off
								System.out.print(Ansi.AUTO.string("""
										%s Maximum invalid hours (%d) reached: ending search
										""".formatted(
												  streamMessagePrefix
												, maxStreamInvalid
											)
										));
								// @formatter:on
								return;
							}
						}
					}
				} else {
					// bisect to narrow down the difference and report results
					final long rangeHalfHours = rangeHours / 2;
					DatumStreamTimeRange leftRange = range.startingHoursRange(rangeHalfHours);
					DatumStreamTimeRange rightRange = range.endingHoursRange(rangeHalfHours);
					findDifferences(leftRange);
					findDifferences(rightRange);
				}
			}

		}

		private DatumStreamValidationInfo queryDifference(DatumStreamTimeRange range, Aggregation aggregation,
				Aggregation partialAggregation) {
			final Datum expected = RestUtils.readingDifference(restClient, nodeAndSource, range.start(), range.end(),
					properties);
			final Datum rollup = RestUtils.readingDifferenceRollup(restClient, nodeAndSource, range.start(),
					range.end(), properties, aggregation, partialAggregation);

			return new DatumStreamValidationInfo(expected, rollup);
		}

	}

}
