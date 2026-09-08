package benchmark;

import java.util.ArrayList;
import java.util.List;

final class BenchmarkOrchestrator {
    private final StreamsRunner streams = new StreamsRunner();
    private final PlainRunner plain = new PlainRunner();
    private final ReportWriter reports = new ReportWriter();

    void run(Config config) throws Exception {
        List<BenchmarkResult> results = new ArrayList<>();
        List<SkippedScenario> skipped = new ArrayList<>();
        for (int requestedPartitions : config.partitions()) {
            int actualPartitions = KafkaSupport.ensurePartitions(config, requestedPartitions);
            for (int eventCount : config.eventCounts()) {
                if (shouldSkip(requestedPartitions, actualPartitions)) {
                    skipped.add(new SkippedScenario(eventCount, requestedPartitions, actualPartitions,
                            "The retained topics already have " + actualPartitions + " partitions; Kafka partitions cannot be decreased."));
                    reports.write(config.resultsDir(), results, skipped);
                    continue;
                }
                System.out.printf("Running %,d events with %d partitions%n", eventCount, actualPartitions);
                if (config.warmupEvents() > 0) {
                    streams.run(config, config.warmupEvents(), requestedPartitions, actualPartitions, 0, seed(eventCount, requestedPartitions, 0));
                    plain.run(config, config.warmupEvents(), requestedPartitions, actualPartitions, 0, seed(eventCount, requestedPartitions, 0));
                }
                for (int iteration = 1; iteration <= config.iterations(); iteration++) {
                    long seed = seed(eventCount, requestedPartitions, iteration);
                    results.add(streams.run(config, eventCount, requestedPartitions, actualPartitions, iteration, seed));
                    reports.write(config.resultsDir(), results, skipped);
                    results.add(plain.run(config, eventCount, requestedPartitions, actualPartitions, iteration, seed));
                    reports.write(config.resultsDir(), results, skipped);
                }
            }
        }
        reports.write(config.resultsDir(), results, skipped);
    }

    static boolean shouldSkip(int requested, int actual) { return actual > requested; }

    private static long seed(int events, int partitions, int iteration) {
        return 42L + events * 31L + partitions * 17L + iteration;
    }
}
