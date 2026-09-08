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
                for (long inputRate : config.inputRates()) {
                    for (int processingThreads : config.processingThreads()) {
                        runScenario(config, results, skipped, eventCount, requestedPartitions,
                                actualPartitions, processingThreads, inputRate);
                    }
                }
            }
        }
        reports.write(config.resultsDir(), results, skipped);
    }

    static boolean shouldSkip(int requested, int actual) { return actual > requested; }

    private void runScenario(Config config, List<BenchmarkResult> results, List<SkippedScenario> skipped,
                             int eventCount, int requestedPartitions, int actualPartitions,
                             int processingThreads, long inputRate) throws Exception {
        if (shouldSkip(requestedPartitions, actualPartitions)) {
            skipped.add(new SkippedScenario(eventCount, requestedPartitions, actualPartitions,
                    processingThreads, inputRate,
                    "The retained topics already have " + actualPartitions
                            + " partitions; Kafka partitions cannot be decreased."));
            reports.write(config.resultsDir(), results, skipped);
            return;
        }

        String rate = inputRate == 0 ? "unthrottled" : String.format("%,d events/s", inputRate);
        System.out.printf("Running %,d events, %d partitions, %d consumers, %s%n",
                eventCount, actualPartitions, processingThreads, rate);
        if (config.warmupEvents() > 0) {
            long seed = seed(eventCount, requestedPartitions, processingThreads, inputRate, 0);
            streams.run(config, config.warmupEvents(), requestedPartitions, actualPartitions,
                    processingThreads, inputRate, 0, seed);
            plain.run(config, config.warmupEvents(), requestedPartitions, actualPartitions,
                    processingThreads, inputRate, 0, seed);
        }
        for (int iteration = 1; iteration <= config.iterations(); iteration++) {
            long seed = seed(eventCount, requestedPartitions, processingThreads, inputRate, iteration);
            results.add(streams.run(config, eventCount, requestedPartitions, actualPartitions,
                    processingThreads, inputRate, iteration, seed));
            reports.write(config.resultsDir(), results, skipped);
            results.add(plain.run(config, eventCount, requestedPartitions, actualPartitions,
                    processingThreads, inputRate, iteration, seed));
            reports.write(config.resultsDir(), results, skipped);
        }
    }

    private static long seed(int events, int partitions, int processingThreads, long inputRate, int iteration) {
        return 42L + events * 31L + partitions * 17L + processingThreads * 13L + inputRate * 7L + iteration;
    }
}
