package benchmark;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.AppInfoParser;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

final class RunSupport {
    private RunSupport() {}

    static Generation generate(Config config, String runId, int durationSeconds, long seed,
                               int partitions) throws Exception {
        if (config.processingMode() == ProcessingMode.VEHICLE_CONGESTION)
            return generateVehicleCongestion(config, runId, durationSeconds);
        AtomicReference<Exception> failure = new AtomicReference<>();
        Map<Long, Workload.ExpectedWindowAccumulator> expectedWindows = new HashMap<>();
        Map<Integer, String> expectedAggregates = new HashMap<>();
        java.util.List<Integer> schedule = Workload.inputSchedule(durationSeconds,
                config.minInputsPerSecond(), config.maxInputsPerSecond(), seed);
        int sequence = 0;
        long identitySequence = 0;
        java.util.SplittableRandom duplicates = new java.util.SplittableRandom(seed ^ 0x5deece66dL);
        InputEvent previous = null;
        long started = System.nanoTime();
        try (var producer = KafkaSupport.producer(config)) {
            for (int second = 0; second < schedule.size(); second++) {
                long window = second / config.outputIntervalSeconds();
                if (config.processingMode() == ProcessingMode.METADATA) {
                    expectedWindows.computeIfAbsent(window, ignored -> new Workload.ExpectedWindowAccumulator());
                }
                if (second % config.outputIntervalSeconds() == 0) previous = null;
                int count = schedule.get(second);
                for (int index = 0; index < count; index++) {
                    waitUntil(started + second * 1_000_000_000L
                            + (long) index * 1_000_000_000L / Math.max(1, count));
                    long now = System.currentTimeMillis();
                    boolean duplicate = config.processingMode() == ProcessingMode.METADATA && previous != null
                            && duplicates.nextInt(100) < config.duplicatePercent();
                    InputEvent event;
                    if (duplicate) {
                        event = new InputEvent(previous.eventId(), runId, sequence, window,
                                previous.key(), now, previous.payload());
                    } else {
                        event = Workload.event(runId, sequence, identitySequence++, window,
                                config.payloadBytes(), config.uniqueKeys(), seed, now);
                        previous = event;
                    }
                    if (config.processingMode() == ProcessingMode.METADATA) {
                        expectedWindows.computeIfAbsent(window, ignored -> new Workload.ExpectedWindowAccumulator())
                                .add(event, duplicate);
                    }
                    long eventTime = second * 1_000L + (long) index * 1_000L / Math.max(1, count);
                    producer.send(new ProducerRecord<>(config.inputTopic(), null, eventTime,
                                    event.key(), EventCodec.write(event)),
                            (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                    sequence++;
                }
                waitUntil(started + (second + 1L) * 1_000_000_000L);
                boolean windowComplete = (second + 1) % config.outputIntervalSeconds() == 0
                        || second + 1 == schedule.size();
                if (config.processingMode() == ProcessingMode.METADATA && windowComplete) {
                    Workload.ExpectedWindowAccumulator expected = expectedWindows.remove(window);
                    expectedAggregates.put(Math.toIntExact(window), expected.output(runId, window,
                            config.outputIntervalSeconds(), durationSeconds, 0).payload());
                    for (int partition = 0; partition < partitions; partition++) {
                        InputEvent marker = new InputEvent("marker-" + window + '-' + partition,
                                runId, -1, window, "marker-" + partition, System.currentTimeMillis(), "");
                        producer.send(new ProducerRecord<>(config.inputTopic(), partition,
                                        (window + 1) * config.outputIntervalSeconds() * 1_000L,
                                        marker.key(), EventCodec.write(marker)),
                                (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                    }
                }
            }
            producer.flush();
            if (failure.get() != null) throw failure.get();
            int expectedOutputs = config.processingMode() == ProcessingMode.METADATA
                    ? expectedAggregates.size() : sequence;
            return new Generation(sequence, expectedOutputs, Map.copyOf(expectedAggregates));
        }
    }

    private static Generation generateVehicleCongestion(Config config, String runId,
                                                         int durationSeconds) throws Exception {
        AtomicReference<Exception> failure = new AtomicReference<>();
        Map<Integer, String> expected = new HashMap<>();
        Map<String, VehicleCongestionRule> rules = new HashMap<>();
        int sent = 0;
        long started = System.nanoTime();
        long epochStarted = System.currentTimeMillis();
        try (var producer = KafkaSupport.producer(config)) {
            int frames = durationSeconds * config.framesPerSecond();
            for (int frameIndex = 0; frameIndex < frames; frameIndex++) {
                waitUntil(started + frameIndex * 1_000_000_000L / config.framesPerSecond());
                long timestamp = epochStarted + frameIndex * 1_000L / config.framesPerSecond();
                for (int job = 0; job < config.simulatedJobs(); job++) {
                    FrameEvent frame = VehicleWorkload.frame(config, runId, job, frameIndex, timestamp);
                    FindingPayload finding = rules.computeIfAbsent(frame.jobId(), ignored -> new VehicleCongestionRule(config))
                            .process(frame);
                    if (finding != null) {
                        OutputEvent output = VehicleWorkload.finding(runId, finding, 0);
                        expected.put(Math.toIntExact(output.sequenceNumber()), output.payload());
                    }
                    producer.send(new ProducerRecord<>(config.inputTopic(), frame.jobId(), EventCodec.write(frame)),
                            (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                    sent++;
                }
            }
            waitUntil(started + durationSeconds * 1_000_000_000L);
            producer.flush();
            if (failure.get() != null) throw failure.get();
        }
        return new Generation(sent, expected.size(), Map.copyOf(expected));
    }

    private static void waitUntil(long targetNanos) throws InterruptedException {
        long remaining;
        while ((remaining = targetNanos - System.nanoTime()) > 0) {
            if (remaining > 2_000_000) Thread.sleep(Math.min(remaining / 1_000_000, 10));
            else Thread.onSpinWait();
        }
    }

    static BenchmarkResult result(Config config, String implementation, String runId, int iteration,
                                  int requestedPartitions, int actualPartitions,
                                  int serviceInstances, java.util.List<Integer> eventsConsumedPerService,
                                  Generation generation, StageMetrics metrics,
                                  long processingStartedNanos, long finishedNanos, ResourceUsage resources,
                                  Validation validation, long gcCount, long gcTime) {
        double elapsed = (finishedNanos - processingStartedNanos) / 1_000_000_000.0;
        var runtime = ManagementFactory.getRuntimeMXBean();
        String gc = ManagementFactory.getGarbageCollectorMXBeans().stream().map(bean -> bean.getName()).sorted()
                .reduce((a, b) -> a + ", " + b).orElse("unknown");
        RuntimeDetails details = new RuntimeDetails(System.getProperty("java.version"), System.getProperty("java.vendor"),
                System.getProperty("java.vm.name"), AppInfoParser.getVersion(), gc,
                Runtime.getRuntime().maxMemory() / 1024 / 1024 * serviceInstances,
                String.join(" ", runtime.getInputArguments()), gcCount, gcTime);
        return new BenchmarkResult(implementation, java.time.Instant.now().toString(), runId, iteration, generation.sent(),
                config.durationSeconds(), config.outputIntervalSeconds(),
                requestedPartitions, actualPartitions,
                config.processingMode() == ProcessingMode.VEHICLE_CONGESTION ? 0 : config.payloadBytes(), serviceInstances,
                eventsConsumedPerService,
                metrics.elapsedSeconds(metrics.firstIngestNanos, metrics.lastIngestNanos),
                metrics.throughput(metrics.firstIngestNanos, metrics.lastIngestNanos, validation.consumed()),
                metrics.elapsedSeconds(metrics.firstProcessNanos, metrics.lastProcessNanos),
                metrics.throughput(metrics.firstProcessNanos, metrics.lastProcessNanos, validation.consumed()),
                Statistics.latency(metrics.processing),
                Statistics.latency(metrics.flushing),
                metrics.elapsedSeconds(metrics.firstPublishNanos, metrics.lastPublishNanos),
                metrics.throughput(metrics.firstPublishNanos, metrics.lastPublishNanos, validation.observed()),
                Statistics.latency(metrics.publishing), elapsed, elapsed == 0 ? 0 : validation.consumed() / elapsed,
                resources, validation, details, config.safeConfiguration());
    }

    static String runId(String implementation) {
        return implementation.toLowerCase().replace(' ', '-') + '-' + UUID.randomUUID();
    }
}

record Generation(int sent, int expectedOutputs, Map<Integer, String> expectedAggregates) {}
