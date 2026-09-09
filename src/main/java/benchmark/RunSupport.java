package benchmark;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.AppInfoParser;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

final class RunSupport {
    private RunSupport() {}

    static Generation generate(Config config, String runId, int durationSeconds, long inputRate, long seed,
                               java.util.function.IntSupplier observed) throws Exception {
        long started = System.nanoTime();
        long deadline = started + durationSeconds * 1_000_000_000L;
        long interval = 1_000_000_000L / inputRate;
        AtomicReference<Exception> failure = new AtomicReference<>();
        int sequence = 0;
        try (var producer = KafkaSupport.producer(config)) {
            while (System.nanoTime() < deadline) {
                long target = started + sequence * interval;
                long remaining;
                while ((remaining = target - System.nanoTime()) > 0) {
                    if (remaining > 1_000_000) Thread.sleep(Math.min(remaining / 1_000_000, 10));
                    else Thread.onSpinWait();
                }
                long now = System.currentTimeMillis();
                InputEvent event = Workload.event(runId, sequence, config.payloadBytes(), config.uniqueKeys(), seed, now);
                producer.send(new ProducerRecord<>(config.inputTopic(), null, now, event.key(), EventCodec.write(event)),
                        (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
                sequence++;
            }
            int observedAtWindowEnd = observed.getAsInt();
            long windowEnded = System.nanoTime();
            producer.flush();
            long finished = System.nanoTime();
            if (failure.get() != null) throw failure.get();
            double windowSeconds = (windowEnded - started) / 1_000_000_000.0;
            return new Generation(sequence, windowSeconds == 0 ? 0 : sequence / windowSeconds,
                    started, windowEnded, finished, observedAtWindowEnd,
                    (finished - windowEnded) / 1_000_000_000.0);
        }
    }

    static BenchmarkResult result(Config config, String implementation, String runId, int iteration,
                                  int measurementSeconds, int requestedPartitions, int actualPartitions,
                                  int serviceInstances, java.util.List<Integer> eventsConsumedPerService,
                                  long inputRate, Generation generation, int backlogAtGenerationEnd,
                                  StageMetrics metrics, long finishedNanos, ResourceUsage resources,
                                  Validation validation, long gcCount, long gcTime) {
        double elapsed = (finishedNanos - generation.startedNanos()) / 1_000_000_000.0;
        double catchUpSeconds = Math.max(0, (finishedNanos - generation.windowEndedNanos()) / 1_000_000_000.0);
        var runtime = ManagementFactory.getRuntimeMXBean();
        String gc = ManagementFactory.getGarbageCollectorMXBeans().stream().map(bean -> bean.getName()).sorted()
                .reduce((a, b) -> a + ", " + b).orElse("unknown");
        RuntimeDetails details = new RuntimeDetails(System.getProperty("java.version"), System.getProperty("java.vendor"),
                System.getProperty("java.vm.name"), AppInfoParser.getVersion(), gc,
                Runtime.getRuntime().maxMemory() / 1024 / 1024 * serviceInstances,
                String.join(" ", runtime.getInputArguments()), gcCount, gcTime);
        return new BenchmarkResult(implementation, java.time.Instant.now().toString(), runId, iteration, generation.sent(),
                measurementSeconds, requestedPartitions, actualPartitions, config.payloadBytes(), serviceInstances,
                eventsConsumedPerService, inputRate, generation.achievedRate(), generation.producerFlushSeconds(),
                backlogAtGenerationEnd, catchUpSeconds,
                metrics.throughput(metrics.firstIngestNanos, metrics.lastIngestNanos, validation.consumed()),
                Statistics.latency(metrics.ingestion),
                metrics.throughput(metrics.firstProcessNanos, metrics.lastProcessNanos, validation.consumed()),
                Statistics.latency(metrics.processing),
                metrics.throughput(metrics.firstPublishNanos, metrics.lastPublishNanos, validation.observed()),
                Statistics.latency(metrics.publishing), elapsed, elapsed == 0 ? 0 : validation.observed() / elapsed,
                Statistics.latency(metrics.endToEnd), resources, validation, details, config.safeConfiguration());
    }

    static String runId(String implementation) {
        return implementation.toLowerCase().replace(' ', '-') + '-' + UUID.randomUUID();
    }
}

record Generation(int sent, double achievedRate, long startedNanos, long windowEndedNanos, long finishedNanos,
                  int observedAtWindowEnd, double producerFlushSeconds) {}
