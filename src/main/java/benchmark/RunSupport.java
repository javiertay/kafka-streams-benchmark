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

    static Generation generate(Config config, String runId, int eventCount, long seed,
                               int partitions) throws Exception {
        AtomicReference<Exception> failure = new AtomicReference<>();
        Map<String, long[]> expectedMetadataOutputs = new HashMap<>();
        int sequence = 0;
        try (var producer = KafkaSupport.producer(config)) {
            while (sequence < eventCount) {
                long now = System.currentTimeMillis();
                long eventSequence = config.processingMode() == ProcessingMode.METADATA
                        ? Workload.metadataSequence(sequence) : sequence;
                InputEvent event = Workload.event(
                        runId, eventSequence, config.payloadBytes(), config.uniqueKeys(), seed, now);
                long eventTime = config.processingMode() == ProcessingMode.METADATA
                        ? Workload.eventTime(eventSequence, eventCount) : now;
                if (config.processingMode() == ProcessingMode.METADATA) {
                    if (eventSequence == sequence) {
                        expectedMetadataOutputs.compute(Workload.aggregateKey(eventTime, event.key()),
                                (key, aggregate) -> new long[] {
                                        eventSequence, aggregate == null ? 1 : aggregate[1] + 1
                                });
                    }
                }
                producer.send(new ProducerRecord<>(
                                config.inputTopic(), null, eventTime, event.key(), EventCodec.write(event)),
                        (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                sequence++;
            }
            if (config.processingMode() == ProcessingMode.METADATA) {
                long markerTimestamp = 2 * Workload.AGGREGATION_WINDOW_MS;
                for (int partition = 0; partition < partitions; partition++) {
                    InputEvent marker = new InputEvent("marker-" + partition, runId, -1,
                            "marker-" + partition, System.currentTimeMillis(), "");
                    producer.send(new ProducerRecord<>(config.inputTopic(), partition, markerTimestamp,
                                    marker.key(), EventCodec.write(marker)),
                            (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                }
            }
            producer.flush();
            if (failure.get() != null) throw failure.get();
            Map<Integer, Long> expectedAggregates = new HashMap<>();
            expectedMetadataOutputs.values().forEach(
                    aggregate -> expectedAggregates.put((int) aggregate[0], aggregate[1]));
            int expectedOutputs = config.processingMode() == ProcessingMode.METADATA
                    ? expectedAggregates.size() : sequence;
            return new Generation(sequence, expectedOutputs, Map.copyOf(expectedAggregates));
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
                requestedPartitions, actualPartitions, config.payloadBytes(), serviceInstances,
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

record Generation(int sent, int expectedOutputs, Map<Integer, Long> expectedAggregates) {}
