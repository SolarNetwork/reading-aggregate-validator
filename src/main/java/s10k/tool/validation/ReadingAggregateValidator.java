package s10k.tool.validation;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.stream.Collectors.joining;
import static net.solarnetwork.domain.datum.Aggregation.Day;
import static net.solarnetwork.domain.datum.Aggregation.Hour;
import static net.solarnetwork.util.DateUtils.ISO_DATE_OPT_TIME_ALT_LOCAL;
import static net.solarnetwork.util.StringNaturalSortComparator.CASE_INSENSITIVE_NATURAL_SORT;
import static org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE;
import static s10k.tool.domain.TimeRangeValidationDifference.differences;

import java.io.FileWriter;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException.TooManyRequests;
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
import s10k.tool.domain.DatumStreamValidationResult;
import s10k.tool.domain.LocalDateTimeRange;
import s10k.tool.domain.NodeAndSource;
import s10k.tool.domain.PropertyValueComparison;
import s10k.tool.domain.TimeRangeValidationDifference;
import s10k.tool.support.RestUtils;

/**
 * Validate SolarNetwork aggregate reading values.
 */
@Component
@Command(name = "validate")
public class ReadingAggregateValidator implements Callable<Integer> {

	private static final long HOURS_PER_DAY = 24L;

	@Option(names = { "-v", "--verbose" }, description = "verbose output")
	boolean[] verbosity;

	@Option(names = { "--http-trace" }, description = "trace HTTP exchanges")
	boolean traceHttp;

	@Option(names = { "-j", "--threads" }, description = "number of concurrent threads", defaultValue = "1")
	int threadCount = 1;

	@Option(names = { "-h", "--help" }, usageHelp = true, description = "display this help message")
	boolean usageHelpRequested;

	@Option(names = { "-node",
			"--node-id" }, description = "a node ID to validate", split = "\\s*,\\s*", splitSynopsisLabel = ",")
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

	@Option(names = { "-R", "--newest-to-oldest" }, description = "process data in a reverse time fashion")
	boolean newestToOldest;

	@Option(names = {
			"--compensate-higher-agg" }, description = "compensate for higher aggregation levels reporting differences that lower levels do not")
	boolean compensateForHigherAggregations;

	@Option(names = { "-i", "--incremental-mark-stale" }, description = "incrementally mark individual stream results")
	boolean incrementalMarkStale;

	private final ClientHttpRequestFactory reqFactory;
	private final ObjectMapper objectMapper;

	private ExecutorService resultProcessor;
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

		if (verbosity != null) {
			// @formatter:off
			System.out.println(Ansi.AUTO.string("""
					@|bold Nodes:|@   %s
					@|bold Sources:|@ %s
					""".formatted(
							streams.stream().map(NodeAndSource::nodeId).distinct().sorted()
								.map(v -> "@|yellow %d|@".formatted(v)).collect(joining(", ")),
							streams.stream().map(NodeAndSource::sourceId).distinct().sorted(CASE_INSENSITIVE_NATURAL_SORT)
								.map(v -> "@|yellow %s|@".formatted(v)).collect(joining(", "))
					)));
			// @formatter:on
		}

		if (incrementalMarkStale) {
			this.resultProcessor = (threadCount > 1 ? Executors.newFixedThreadPool(threadCount)
					: Executors.newSingleThreadExecutor());
		}

		List<Future<DatumStreamValidationResult>> taskResults = new ArrayList<>();

		try (ExecutorService threadPool = (threadCount > 1 ? Executors.newFixedThreadPool(threadCount)
				: Executors.newSingleThreadExecutor())) {
			for (NodeAndSource streamIdent : streams) {
				final String streamIdentMessagePrefix = nodeAndSourceMessagePrefix(streamIdent, streams);
				taskResults
						.add(threadPool.submit(new StreamValidator(restClient, streamIdent, streamIdentMessagePrefix)));
			}
			threadPool.shutdown();
			boolean finished = threadPool.awaitTermination(maxWait.toSeconds(), TimeUnit.SECONDS);
			if (!finished) {
				System.out.println("Validation tasks did not complete within %ds.".formatted(maxWait.toSeconds()));
				globalStop = true;
				threadPool.shutdownNow();
			}
		}

		final List<DatumStreamValidationResult> results = new ArrayList<>();
		boolean differencesFound = false;

		for (Future<DatumStreamValidationResult> taskResult : taskResults) {
			if (!taskResult.isDone()) {
				continue;
			}
			try {
				DatumStreamValidationResult result = taskResult.get();
				if (result.hasDifferences()) {
					differencesFound = true;
				}
				results.add(result);
			} catch (ExecutionException e) {
				Throwable cause = e.getCause();
				String msg;
				if (cause instanceof CancellationException) {
					msg = "cancelled from timeout";
				} else {
					msg = cause.toString();
				}
				System.err.println("Validation task failed: " + msg);
			}
		}

		results.sort(null);

		try (ICsvListWriter csv = new CsvListWriter(
				differencesFound && reportFileName != null ? new FileWriter(reportFileName, UTF_8) : new NullWriter(),
				STANDARD_PREFERENCE)) {
			csv.writeHeader("Node", "Source", "Start", "End", "Property", "Expected", "Actual", "Difference");

			for (DatumStreamValidationResult result : results) {
				final NodeAndSource streamIdent = result.nodeAndSource();
				final String streamIdentMessagePrefix = nodeAndSourceMessagePrefix(streamIdent, streams);

				if (!result.hasDifferences()) {
					System.out.println(Ansi.AUTO
							.string("%s @|green No validation problems found|@".formatted(streamIdentMessagePrefix)));
					continue;
				}

				final List<TimeRangeValidationDifference> invalidHours = result.invalidHours();

				// @formatter:off
				System.out.print(Ansi.AUTO.string("""
						%s @|red %d|@ validation problems found
						""".formatted(
								  streamIdentMessagePrefix
								, invalidHours.size()
							)
						));
				// @formatter:on

				for (TimeRangeValidationDifference invalidHour : invalidHours) {
					boolean repeat = false;
					for (Entry<String, PropertyValueComparison> diffEntry : invalidHour.differences().entrySet()) {
						String[] row = new String[8];
						if (!repeat) {
							row[0] = streamIdent.nodeId().toString();
							row[1] = streamIdent.sourceId();
							row[2] = ISO_DATE_OPT_TIME_ALT_LOCAL.format(invalidHour.range().start());
							row[3] = ISO_DATE_OPT_TIME_ALT_LOCAL.format(invalidHour.range().end());
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

				if (resultProcessor != null) {
					handleResultMarkStale(restClient, result, streamIdentMessagePrefix);
				}
			}
		}

		if (resultProcessor != null) {
			resultProcessor.shutdown();
			boolean finished = resultProcessor.awaitTermination(maxWait.toSeconds(), TimeUnit.SECONDS);
			if (!finished) {
				System.out.println("Mark stale tasks did not complete within %ds.".formatted(maxWait.toSeconds()));
				resultProcessor.shutdownNow();
			}
		}

		return 0;
	}

	private void handleResultMarkStale(final RestClient restClient, final DatumStreamValidationResult result,
			final String streamIdentMessagePrefix) {
		final NodeAndSource streamIdent = result.nodeAndSource();
		SortedSet<LocalDateTimeRange> staleTimeRanges = result.uniqueHourTimeRanges();
		if (!staleTimeRanges.isEmpty()) {
			// @formatter:off
			System.out.print(Ansi.AUTO.string("""
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

	private static String nodeAndSourceMessagePrefix(NodeAndSource nodeAndSource, List<NodeAndSource> allStreams) {
		int nodeIdWidth = 1;
		int sourceIdWidth = 1;
		for (NodeAndSource streamIdent : allStreams) {
			int w = streamIdent.nodeId().toString().length();
			if (w > nodeIdWidth) {
				nodeIdWidth = w;
			}
			w = streamIdent.sourceId().length();
			if (w > sourceIdWidth) {
				sourceIdWidth = w;
			}
		}
		return ("[@|yellow %" + nodeIdWidth + "d|@ @|yellow %-" + sourceIdWidth + "s|@]")
				.formatted(nodeAndSource.nodeId(), nodeAndSource.sourceId());
	}

	private final class StreamValidator implements Callable<DatumStreamValidationResult> {

		private final RestClient restClient;
		private final NodeAndSource nodeAndSource;
		private final String streamMessagePrefix;

		private final List<TimeRangeValidationDifference> results = new ArrayList<>();

		private ZoneId zone = ZoneOffset.UTC;
		private long invalidHours = 0L;
		private boolean stop;

		private StreamValidator(RestClient restClient, NodeAndSource nodeAndSource, String streamMessagePrefix) {
			super();
			this.restClient = restClient;
			this.nodeAndSource = nodeAndSource;
			this.streamMessagePrefix = streamMessagePrefix;
		}

		@Override
		public DatumStreamValidationResult call() throws Exception {
			if (verbosity != null) {
				System.out.println(Ansi.AUTO.string(streamMessagePrefix + " Validation starting"));
			}

			final DatumStreamTimeRange range = RestUtils.datumStreamTimeRange(restClient, nodeAndSource,
					Instant.now().minus(minDaysOffsetFromNow, DAYS).truncatedTo(DAYS));
			if (range == null) {
				System.out.println(Ansi.AUTO.string(streamMessagePrefix + " Time range not available"));
				return DatumStreamValidationResult.emptyValidationResult(nodeAndSource, zone);
			}

			zone = range.zone();

			if (verbosity != null) {
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

			if (verbosity != null) {
				System.out.println(Ansi.AUTO.string("%s Validation complete".formatted(streamMessagePrefix)));
			}
			DatumStreamValidationResult result = new DatumStreamValidationResult(nodeAndSource, zone, results);
			if (resultProcessor != null) {
				resultProcessor.submit(() -> {
					handleResultMarkStale(restClient, result, streamMessagePrefix);
				});
			}
			return result;
		}

		private boolean findDifferences(DatumStreamTimeRange range) {
			if (stop || globalStop) {
				return false;
			}
			final long rangeHours = range.hourCount();
			if (rangeHours < 1) {
				return false;
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

			boolean hourInvalidationsFound = false;

			final TimeRangeValidationDifference diff = queryDifference(range, aggregation, partialAggregation);
			if (diff.hasDifferences()) {
				results.add(diff);

				if (verbosity != null || range.hourCount() > 1) {
					// @formatter:off
					if (verbosity != null && verbosity.length > 1) {
						System.out.print(Ansi.AUTO.string("""
								%s Difference discovered in range %s - %s (%s; %d days): %s %s
								""".formatted(
										  streamMessagePrefix
										, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.start())
										, ISO_DATE_OPT_TIME_ALT_LOCAL.format(range.end())
										, range.zone().getId()
										, DAYS.between(range.start(), range.end())
										, aggregation
										, diff.differences()
									)
								));
					} else {
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
					}
					// @formatter:on
				}

				if (rangeHours <= HOURS_PER_DAY) {
					// reach final hour-level aggregate comparison so iterate over hours
					for (LocalDateTime hour = range.start(); hour.isBefore(range.end()); hour = hour.plusHours(1)) {
						final DatumStreamTimeRange hourRange = new DatumStreamTimeRange(range.nodeAndSource(),
								range.zone(), new LocalDateTimeRange(hour, hour.plusHours(1)));
						final TimeRangeValidationDifference hourDiff = queryDifference(hourRange, Hour, null);
						if (hourDiff != null && hourDiff.hasDifferences()) {
							hourInvalidationsFound = true;
							if (addInvalidHourShouldStop(hourDiff)) {
								return true;
							}
						}
					}
				} else {
					// bisect to narrow down the difference and report results
					final long rangeHalfHours = rangeHours / 2;
					DatumStreamTimeRange leftRange = range.startingHoursRange(rangeHalfHours);
					DatumStreamTimeRange rightRange = range.endingHoursRange(rangeHalfHours);
					boolean result1 = findDifferences(newestToOldest ? rightRange : leftRange);
					if (stop || globalStop) {
						return hourInvalidationsFound;
					}
					boolean result2 = findDifferences(newestToOldest ? leftRange : rightRange);
					if (compensateForHigherAggregations && (!stop || globalStop) && !(result1 || result2)) {
						// hmm, our overall range was different, but both sub-ranges are not;
						// the higher-level aggregate must be off somewhere, so we need to
						// recompute one hour within every aggregate in the overall range to try to fix
						if (aggregation == Aggregation.Month) {
							if (verbosity != null) {
								// @formatter:off
								System.out.print(Ansi.AUTO.string("""
										%s Difference discovered in range %s - %s (%s; %d days) but not in either half range; invalidating one day/month
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
							for (LocalDateTime hour = range.start().with(TemporalAdjusters.firstDayOfMonth())
									.plusHours(12); hour.isBefore(range.end()); hour = hour.plusMonths(1)) {
								final LocalDateTimeRange hourRange = new LocalDateTimeRange(hour, hour.plusHours(1));
								final Map<String, PropertyValueComparison> syntheticDifferences = new LinkedHashMap<String, PropertyValueComparison>(
										properties.length);
								for (String propertyName : properties) {
									syntheticDifferences.put(propertyName, new PropertyValueComparison(ZERO, ONE));
								}
								hourInvalidationsFound = true;
								if (addInvalidHourShouldStop(new TimeRangeValidationDifference(aggregation, hourRange,
										syntheticDifferences))) {
									return true;
								}
							}
						} else {
							if (verbosity != null) {
								// @formatter:off
								System.out.print(Ansi.AUTO.string("""
										%s Difference discovered in range %s - %s (%s; %d days) but not in either half range; invalidating one hour/day
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
							for (LocalDateTime hour = range.start().truncatedTo(DAYS).plusHours(12); hour
									.isBefore(range.end()); hour = hour.plusDays(1)) {
								final LocalDateTimeRange hourRange = new LocalDateTimeRange(hour, hour.plusHours(1));
								final Map<String, PropertyValueComparison> syntheticDifferences = new LinkedHashMap<>(
										properties.length);
								for (String propertyName : properties) {
									syntheticDifferences.put(propertyName, new PropertyValueComparison(ZERO, ONE));
								}
								hourInvalidationsFound = true;
								if (addInvalidHourShouldStop(new TimeRangeValidationDifference(aggregation, hourRange,
										syntheticDifferences))) {
									return true;
								}
							}
						}
					}
				}
			}

			return hourInvalidationsFound;
		}

		private boolean addInvalidHourShouldStop(TimeRangeValidationDifference diff) {
			results.add(diff);
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
				return true;
			}
			return false;
		}

		private TimeRangeValidationDifference queryDifference(DatumStreamTimeRange range, Aggregation aggregation,
				Aggregation partialAggregation) {
			try {
				final Datum expected = RestUtils.readingDifference(restClient, nodeAndSource, range.start(),
						range.end(), properties);
				final Datum rollup = RestUtils.readingDifferenceRollup(restClient, nodeAndSource, range.start(),
						range.end(), properties, aggregation, partialAggregation);
				return differences(aggregation, range.timeRange(), expected, rollup, properties);
			} catch (TooManyRequests e) {
				// sleep, and then try again
				if (!(stop || globalStop)) {
					try {
						Thread.sleep(1000L);
						if (!(stop || globalStop)) {
							return queryDifference(range, aggregation, partialAggregation);
						}
					} catch (InterruptedException e2) {
						stop = true;
					}
				}
				return null;
			}
		}

	}

}
