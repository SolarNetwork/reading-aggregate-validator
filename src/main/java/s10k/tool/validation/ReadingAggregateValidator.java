package s10k.tool.validation;

import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.stream.Collectors.joining;
import static net.solarnetwork.domain.datum.Aggregation.Day;
import static net.solarnetwork.domain.datum.Aggregation.Hour;
import static net.solarnetwork.domain.datum.Aggregation.Month;
import static net.solarnetwork.domain.datum.Aggregation.None;
import static net.solarnetwork.util.DateUtils.ISO_DATE_OPT_TIME_ALT_LOCAL;
import static net.solarnetwork.util.DateUtils.LOCAL_DATE;
import static net.solarnetwork.util.StringNaturalSortComparator.CASE_INSENSITIVE_NATURAL_SORT;
import static org.supercsv.prefs.CsvPreference.STANDARD_PREFERENCE;
import static s10k.tool.domain.TimeRangeValidationDifference.differences;
import static s10k.tool.domain.ValidationState.Completed;
import static s10k.tool.domain.ValidationState.Incomplete;
import static s10k.tool.domain.ValidationState.NotPerformed;
import static s10k.tool.domain.ValidationState.Processing;

import java.io.FileWriter;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.SequencedCollection;
import java.util.SortedSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.springframework.http.HttpHeaders;
import org.springframework.http.client.ClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException.TooManyRequests;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import org.supercsv.io.CsvListWriter;
import org.supercsv.io.ICsvListWriter;
import org.threeten.extra.Interval;

import com.fasterxml.jackson.databind.ObjectMapper;

import net.solarnetwork.domain.datum.Aggregation;
import net.solarnetwork.domain.datum.Datum;
import net.solarnetwork.io.NullWriter;
import net.solarnetwork.security.Snws2AuthorizationBuilder;
import net.solarnetwork.util.DateUtils;
import net.solarnetwork.web.jakarta.support.StaticAuthorizationCredentialsProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Help.Ansi;
import picocli.CommandLine.Option;
import s10k.tool.domain.DatumStreamTimeRange;
import s10k.tool.domain.DatumStreamValidationResult;
import s10k.tool.domain.NodeAndSource;
import s10k.tool.domain.PropertyValueComparison;
import s10k.tool.domain.TimeRangeValidationDifference;
import s10k.tool.domain.ValidationState;
import s10k.tool.support.RestUtils;

/**
 * Validate SolarNetwork aggregate reading values.
 */
@Component
@Command(name = "validate")
public class ReadingAggregateValidator implements Callable<Integer> {

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
			"--min-days-offset" }, description = "minimum number of days offset from today to disallow validation", defaultValue = "5")
	int minDaysOffsetFromNow = 5;

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
			" " }, description = "compensate for higher aggregation levels reporting differences that lower levels do not")
	boolean compensateForHigherAggregations;

	@Option(names = {
			"--generate-reset-datum" }, description = "generate Reset datum auxiliary records when --compensate-higher-agg enabled")
	boolean generateAuxiliaryResetDatum;

	@Option(names = { "-i", "--incremental-mark-stale" }, description = "incrementally mark individual stream results")
	boolean incrementalMarkStale;

	private static final Duration HALF_YEAR = Duration.ofDays(183);

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
				try {
					// sleep for a bit for tasks to pick up globalStop
					Thread.sleep(2000L);
				} catch (InterruptedException e) {
					// continue;
				}
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

				if (result.validationState() == ValidationState.NotPerformed) {
					System.out.println(
							Ansi.AUTO.string("%s Validation not performed".formatted(streamIdentMessagePrefix)));
					continue;
				} else if (!result.hasDifferences()) {
					if (result.validationState() == ValidationState.Completed) {
						System.out.println(Ansi.AUTO.string(
								"%s @|green No validation problems found|@".formatted(streamIdentMessagePrefix)));
					} else {
						System.out.println(Ansi.AUTO.string("%s No validation problems found (not completely processed)"
								.formatted(streamIdentMessagePrefix)));
					}
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
							row[2] = ISO_DATE_OPT_TIME_ALT_LOCAL
									.format(invalidHour.range().getStart().atZone(result.zone()).toLocalDateTime());
							row[3] = ISO_DATE_OPT_TIME_ALT_LOCAL
									.format(invalidHour.range().getEnd().atZone(result.zone()).toLocalDateTime());
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

				if (!incrementalMarkStale) {
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
		SortedSet<Interval> staleTimeRanges = result.uniqueHourTimeRanges();
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

			for (Interval staleTimeRange : staleTimeRanges) {
				URI markStaleUri = RestUtils.markStaleUri(streamIdent, staleTimeRange);
				if (markStale) {
					boolean marked = RestUtils.markStale(restClient, streamIdent, staleTimeRange);
					if (marked) {
						// @formatter:off
						System.out.print(Ansi.AUTO.string("""
								%s Marked range %s - %s as stale (%d hours)
								""".formatted(
										  streamIdentMessagePrefix
										, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.getStart().atZone(result.zone()).toLocalDateTime())
										, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.getEnd().atZone(result.zone()).toLocalDateTime())
										, HOURS.between(staleTimeRange.getStart(), staleTimeRange.getEnd())
									)
								));
						// @formatter:on
					} else {
						// @formatter:off
						System.out.print(Ansi.AUTO.string("""
								%s @|Error|@ marking range %s - %s as stale (%d hours)
								""".formatted(
										  streamIdentMessagePrefix
										, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.getStart().atZone(result.zone()).toLocalDateTime())
										, ISO_DATE_OPT_TIME_ALT_LOCAL.format(staleTimeRange.getEnd().atZone(result.zone()).toLocalDateTime())
										, HOURS.between(staleTimeRange.getStart(), staleTimeRange.getEnd())
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

		private final AtomicReference<ValidationState> state = new AtomicReference<>(NotPerformed);
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
			final DatumStreamValidationResult result = new DatumStreamValidationResult(nodeAndSource, zone, state,
					results);
			if (!(stop || globalStop)) {
				result.state().set(Processing);
				executeValidation();
				result.state().compareAndSet(Processing, Completed);
			}
			if (resultProcessor != null) {
				@SuppressWarnings("unused")
				var unused = resultProcessor.submit(() -> {
					handleResultMarkStale(restClient, result, streamMessagePrefix);
				});
			}
			return result;
		}

		private void executeValidation() {
			if (verbosity != null) {
				System.out.println(Ansi.AUTO.string(streamMessagePrefix + " Validation starting"));
			}

			final DatumStreamTimeRange range = datumStreamTimeRange(null,
					Instant.now().minus(minDaysOffsetFromNow, DAYS).truncatedTo(HOURS));
			if (range == null) {
				System.out.println(Ansi.AUTO.string(streamMessagePrefix + " Time range not available"));
				return;
			}

			zone = range.zone();

			if (verbosity != null) {
				// @formatter:off
				System.out.print(Ansi.AUTO.string("""
						%s Stream range discovered: %s - %s (%s; %d days)
						""".formatted(
								  streamMessagePrefix
								, LOCAL_DATE.format(range.startLocal())
								, LOCAL_DATE.format(range.endLocal())
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
		}

		private boolean findDifferences(DatumStreamTimeRange range) {
			if (stop || globalStop) {
				state.set(Incomplete);
				return false;
			}
			if (range == null) {
				return false;
			}
			final long rangeDays = range.dayCount();
			if (rangeDays < 1) {
				return false;
			}

			final long rangeMonths = range.monthCount();
			final Aggregation aggregation;
			final Aggregation partialAggregation;
			if (rangeMonths > 1) {
				aggregation = Month;
				partialAggregation = Day;
			} else {
				aggregation = Day;
				partialAggregation = Hour;
			}

			boolean differencesFound = false;

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
										, LOCAL_DATE.format(range.startLocal())
										, LOCAL_DATE.format(range.endLocal())
										, range.zone().getId()
										, DAYS.between(range.startLocal(), range.endLocal())
										, aggregation
										, diff.differences()
									)
								));
					} else {
						System.out.print(Ansi.AUTO.string("""
								%s Difference discovered in range %s - %s (%s; %d days)
								""".formatted(
										  streamMessagePrefix
										, LOCAL_DATE.format(range.startLocal())
										, LOCAL_DATE.format(range.endLocal())
										, range.zone().getId()
										, DAYS.between(range.startLocal(), range.endLocal())
									)
								));
					}
					// @formatter:on
				}

				if (rangeDays <= 1) {
					// load entire range of hour aggregates in one query
					final NavigableMap<Instant, Datum> hourAggregates = readingDifferenceAggregates(range);

					// then reach final hour-level aggregate comparison so iterate over hours
					for (Instant hour = range.start(); hour.isBefore(range.end()); hour = hour.plus(1L, HOURS)) {
						final DatumStreamTimeRange hourRange = new DatumStreamTimeRange(range.nodeAndSource(),
								range.zone(), Interval.of(hour, hour.plus(1L, HOURS)));
						final TimeRangeValidationDifference hourDiff = queryDifference(hourRange, Hour,
								hourAggregates.get(hour.atZone(zone).toInstant()));
						if (hourDiff != null && hourDiff.hasDifferences()) {
							differencesFound = true;
							if (addInvalidHourShouldStop(hourDiff)) {
								return true;
							}
						}
					}
				} else {
					// bisect to narrow down the difference and report results
					final long rangeDaysHalf = rangeDays / 2;
					DatumStreamTimeRange leftRange = startingHalfRange(range, rangeDaysHalf);
					DatumStreamTimeRange rightRange = endingHalfRange(range, rangeDaysHalf);
					boolean result1 = findDifferences(newestToOldest ? rightRange : leftRange);
					if (result1) {
						differencesFound = true;
					}
					if (stop || globalStop) {
						state.set(Incomplete);
						return differencesFound;
					}
					boolean result2 = findDifferences(newestToOldest ? leftRange : rightRange);
					if (result2) {
						differencesFound = true;
					}
					if (stop || globalStop) {
						state.set(Incomplete);
						return differencesFound;
					}
					if (compensateForHigherAggregations && !(result1 || result2)) {
						// the overall range was different, but both sub-ranges are not;
						// the higher-level aggregate must be off somewhere, so we need to
						// recompute one hour within every day in the overall range to try to fix
						if (verbosity != null) {
							// @formatter:off
							System.out.print(Ansi.AUTO.string("""
									%s Difference discovered in range %s - %s (%s; %d days) but not either half range:
										Starting half range: %s - %s
										Ending half range:   %s - %s
									""".formatted(
											  streamMessagePrefix
											, LOCAL_DATE.format(range.startLocal())
											, LOCAL_DATE.format(range.endLocal())
											, range.zone().getId()
											, DAYS.between(range.startLocal(), range.endLocal())
											, LOCAL_DATE.format(leftRange.startLocal())
											, LOCAL_DATE.format(leftRange.endLocal())
											, LOCAL_DATE.format(rightRange.startLocal())
											, LOCAL_DATE.format(rightRange.endLocal())
										)
									));
							// @formatter:on
						}

						if (generateAuxiliaryResetDatum) {
							// look for gap with missing Reset datum; first get reading start date for range
							Datum reading = readingDifference(leftRange);

							// now get starting datum + next for that date
							if (reading != null) {
								SequencedCollection<Datum> pair = datum(new DatumStreamTimeRange(nodeAndSource, zone,
										Interval.of(reading.getTimestamp(), range.end())), 2).sequencedValues();

								// find "next" datum after reading start
								if (pair.size() == 2) {
									Datum start = pair.getFirst();
									Datum next = pair.getLast();
									// verify the "next" datum is still within our overall time range
									if (range.timeRange().contains(next.getTimestamp())) {
										// look for NO reset records (bail if any reset records found)
										final Interval resetGap = Interval.of(start.getTimestamp(),
												next.getTimestamp());
										if (!hasDatumAuxiliaryResetRecord(
												new DatumStreamTimeRange(nodeAndSource, zone, resetGap))) {
											TimeRangeValidationDifference resetDiff = differences(None, resetGap, start,
													next, properties);
											if (resetDiff.hasDifferences()) {
												Instant resetRecordDate = saveDatumAuxiliaryResetRecord(nodeAndSource,
														resetDiff);
												if (resetRecordDate != null) {
													PropertyValueComparison firstPropDiff = resetDiff.differences()
															.values().iterator().next();
													System.out
															.println("\tCreated Reset record @ %s (%s -> %s)".formatted(
																	DateUtils.ISO_DATE_TIME_ALT_UTC
																			.format(resetRecordDate),
																	firstPropDiff.expectedValue(),
																	firstPropDiff.actualValue()));
													// treat this as difference found
													return true;
												}
											}
										}
									}
								}
							}
						}

						System.out.println("\tInvalidating one hour/day");
						for (LocalDateTime hour = range.startLocal().truncatedTo(DAYS), end = range.endLocal(); hour
								.isBefore(end); hour = hour.plusDays(1)) {
							final Instant hourStart = hour.atZone(zone).toInstant();

							// there could be large gaps in the where we split the left/right halves,
							// so skip hours that are not within the two half ranges we resolved
							if (!(leftRange.timeRange().contains(hourStart)
									|| rightRange.timeRange().contains(hourStart))) {
								continue;
							}

							// find first available hour aggregate on given day, then re-process that (so we
							// submit an hour with actual data)
							final NavigableMap<Instant, Datum> hourAggsInDay = readingDifferenceAggregates(
									new DatumStreamTimeRange(nodeAndSource, zone,
											Interval.of(hourStart, hour.plusDays(1).atZone(zone).toInstant())));

							if (hourAggsInDay.isEmpty()) {
								continue;
							}

							final Instant firstHourInDay = hourAggsInDay.firstKey();

							final Interval hourRange = Interval.of(firstHourInDay, firstHourInDay.plus(1, HOURS));
							final Map<String, PropertyValueComparison> syntheticDifferences = new LinkedHashMap<>(
									properties.length);
							for (String propertyName : properties) {
								syntheticDifferences.put(propertyName, new PropertyValueComparison(ZERO, ONE));
							}
							differencesFound = true;
							if (addInvalidHourShouldStop(
									new TimeRangeValidationDifference(Hour, hourRange, syntheticDifferences))) {
								return true;
							}
						}
					}
				}
			}

			return differencesFound;
		}

		private DatumStreamTimeRange startingHalfRange(DatumStreamTimeRange range, long rangeDaysHalf) {
			DatumStreamTimeRange halfRange = range.startingDaysRange(rangeDaysHalf);
			DatumStreamTimeRange availRange = datumStreamTimeRange(halfRange.start(), halfRange.end());
			if (availRange != null && Duration.between(availRange.end(), halfRange.end()).compareTo(HALF_YEAR) >= 0) {
				// the split is within a large data gap, so use the available range instead
				return availRange;
			}
			return halfRange;
		}

		private DatumStreamTimeRange endingHalfRange(DatumStreamTimeRange range, long rangeDaysHalf) {
			DatumStreamTimeRange halfRange = range.endingDaysRange(rangeDaysHalf);
			DatumStreamTimeRange availRange = datumStreamTimeRange(halfRange.start(), halfRange.end());
			if (availRange != null
					&& Duration.between(halfRange.start(), availRange.start()).compareTo(HALF_YEAR) >= 0) {
				// the split is within a large data gap, so use the available range instead
				return availRange;
			}
			return halfRange;
		}

		private boolean addInvalidHourShouldStop(TimeRangeValidationDifference diff) {
			results.add(diff);
			invalidHours++;
			if (maxStreamInvalid > 0 && !(invalidHours < maxStreamInvalid)) {
				stop = true;
				state.set(Incomplete);
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

		private <T> T restOp(Supplier<T> provider) {
			try {
				return provider.get();
			} catch (TooManyRequests e) {
				// sleep, and then try again
				if (!(stop || globalStop)) {
					// assume default pause of 1s, but check for a Retry-After header to use;
					// a minimum of 100ms will be applied
					long sleepMs = 1000L;
					HttpHeaders responseHeaders = e.getResponseHeaders();
					if (responseHeaders != null) {
						String retryAfter = responseHeaders.getFirst("X-SN-Rate-Limit-Retry-After");
						if (retryAfter != null) {
							try {
								long retryEpoch = Long.parseLong(retryAfter);
								long retryDiff = retryEpoch - System.currentTimeMillis();
								sleepMs = Math.max(100, Math.min(sleepMs, retryDiff));
							} catch (NumberFormatException nfe) {
								// ignore and just use default
							}
						}
					}
					try {
						Thread.sleep(sleepMs);
						if (!(stop || globalStop)) {
							return restOp(provider);
						}
					} catch (InterruptedException e2) {
						stop = true;
					}
				}
				return null;
			}
		}

		private DatumStreamTimeRange datumStreamTimeRange(Instant minDate, Instant maxDate) {
			return restOp(() -> RestUtils.datumStreamTimeRange(restClient, nodeAndSource, minDate, maxDate));
		}

		private TimeRangeValidationDifference queryDifference(DatumStreamTimeRange range, Aggregation aggregation,
				Aggregation partialAggregation) {
			return restOp(() -> {
				final Datum rollup = RestUtils.readingDifferenceRollup(restClient, range.nodeAndSource(), range.zone(),
						range.start(), range.end(), properties, aggregation, partialAggregation);
				return queryDifference(range, aggregation, rollup);
			});
		}

		private TimeRangeValidationDifference queryDifference(DatumStreamTimeRange range, Aggregation aggregation,
				Datum rollup) {
			return restOp(() -> {
				final Datum expected = RestUtils.readingDifference(restClient, range.nodeAndSource(), range.start(),
						range.end(), properties);
				return differences(aggregation, range.timeRange(), expected, rollup, properties);
			});
		}

		private Datum readingDifference(DatumStreamTimeRange range) {
			return restOp(() -> RestUtils.readingDifference(restClient, range.nodeAndSource(), range.start(),
					range.end(), properties));
		}

		private NavigableMap<Instant, Datum> datum(DatumStreamTimeRange range, Integer limit) {
			return restOp(() -> RestUtils.datum(restClient, range.nodeAndSource(), range.start(), range.end(), limit,
					properties));
		}

		private NavigableMap<Instant, Datum> readingDifferenceAggregates(DatumStreamTimeRange range) {
			return restOp(() -> RestUtils.readingDifferenceAggregates(restClient, range.nodeAndSource(), range.start(),
					range.end(), properties, Hour));
		}

		private boolean hasDatumAuxiliaryResetRecord(DatumStreamTimeRange range) {
			return restOp(
					() -> RestUtils.hasDatumAuxiliaryResetRecord(restClient, range.nodeAndSource(), range.timeRange()));
		}

		private Instant saveDatumAuxiliaryResetRecord(NodeAndSource nodeAndSource,
				TimeRangeValidationDifference resetDiff) {
			return restOp(() -> RestUtils.saveDatumAuxiliaryResetRecord(restClient, nodeAndSource, resetDiff));
		}

	}

}
