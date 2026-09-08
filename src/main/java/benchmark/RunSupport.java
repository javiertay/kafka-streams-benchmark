package benchmark;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.AppInfoParser;

import java.lang.management.ManagementFactory;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

final class RunSupport {
    private RunSupport() {}

    static Generation generate(Config config, String runId, int eventCount, long inputRate, long seed) throws Exception {
        long started = System.nanoTime();
        long interval = inputRate == 0 ? 0 : 1_000_000_000L / inputRate;
        AtomicReference<Exception> failure = new AtomicReference<>();
        try (var producer = KafkaSupport.producer(config)) {
            for (int sequence = 0; sequence < eventCount; sequence++) {
                if (interval > 0) {
                    long target = started + sequence * interval;
                    long remaining;
                    while ((remaining = target - System.nanoTime()) > 0) {
                        if (remaining > 1_000_000) Thread.sleep(Math.min(remaining / 1_000_000, 10));
                        else Thread.onSpinWait();
                    }
                }
                long now = System.currentTimeMillis();
                InputEvent event = Workload.event(runId, sequence, config.payloadBytes(), config.uniqueKeys(), seed, now);
                producer.send(new ProducerRecord<>(config.inputTopic(), null, now, event.key(), EventCodec.write(event)),
                        (metadata, error) -> { if (error != null) failure.compareAndSet(null, error); });
            }
            producer.flush();
        }
        if (failure.get() != null) throw failure.get();
        double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
        return new Generation(eventCount, seconds == 0 ? 0 : eventCount / seconds);
    }

    static BenchmarkResult result(Config config, String implementation, String runId, int iteration,
                                  int eventCount, int requestedPartitions, int actualPartitions,
                                  int processingThreads, long inputRate, Generation generation,
                                  StageMetrics metrics, long finishedNanos,
                                  ResourceUsage resources, Validation validation, long gcCountBefore, long gcTimeBefore) {
        double elapsed = (finishedNanos - metrics.startedNanos) / 1_000_000_000.0;
        var runtime = ManagementFactory.getRuntimeMXBean();
        String gc = ManagementFactory.getGarbageCollectorMXBeans().stream().map(bean -> bean.getName()).sorted()
                .reduce((a, b) -> a + ", " + b).orElse("unknown");
        RuntimeDetails details = new RuntimeDetails(System.getProperty("java.version"), System.getProperty("java.vendor"),
                System.getProperty("java.vm.name"), AppInfoParser.getVersion(), gc,
                Runtime.getRuntime().maxMemory() / 1024 / 1024, String.join(" ", runtime.getInputArguments()),
                Math.max(0, ResourceSampler.gcCount() - gcCountBefore), Math.max(0, ResourceSampler.gcTime() - gcTimeBefore));
        return new BenchmarkResult(implementation, java.time.Instant.now().toString(), runId, iteration, eventCount,
                requestedPartitions, actualPartitions, config.payloadBytes(), processingThreads, inputRate,
                generation.achievedRate(),
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

record Generation(int sent, double achievedRate) {}
