package benchmark;

import java.util.ArrayList;
import java.util.List;

final class BenchmarkOrchestrator {
    private final DistributedRunner streams = new DistributedRunner("Kafka Streams", "streams-");
    private final DistributedRunner plain = new DistributedRunner("Plain Java", "plain-");
    private final ReportWriter reports = new ReportWriter();

    void run(Config config) throws Exception {
        long matrixStarted = System.nanoTime();
        System.out.printf("[benchmark] Waiting for %d Kafka broker%s%n", config.brokerCount(),
                config.brokerCount() == 1 ? "" : "s");
        KafkaSupport.awaitBrokerCount(config);
        System.out.printf("[benchmark] Kafka cluster ready: %d broker%s, replication factor %d%n",
                config.brokerCount(), config.brokerCount() == 1 ? "" : "s", config.replicationFactor());
        System.out.printf("[benchmark] Processing mode: %s%n",
                config.processingMode().name().toLowerCase(java.util.Locale.ROOT));
        int totalScenarios = config.inputRates().size() * config.partitions().stream()
                .mapToInt(partitions -> (int) config.serviceInstances().stream()
                        .filter(services -> isMeaningfulScalingScenario(partitions, services)).count())
                .sum();
        System.out.printf("[benchmark] Starting matrix: %d scenario%s, 2 implementations, "
                        + "%d measured iteration%s, %ds measurement, %ds warm-up%n",
                totalScenarios, totalScenarios == 1 ? "" : "s",
                config.iterations(), config.iterations() == 1 ? "" : "s",
                config.measurementSeconds(), config.warmupSeconds());
        List<BenchmarkResult> results = new ArrayList<>();
        List<SkippedScenario> skipped = new ArrayList<>();
        int scenarioNumber = 0;
        for (int requestedPartitions : config.partitions()) {
            int actualPartitions = KafkaSupport.ensurePartitions(config, requestedPartitions);
            for (long inputRate : config.inputRates()) {
                for (int serviceInstances : config.serviceInstances()) {
                    if (!isMeaningfulScalingScenario(requestedPartitions, serviceInstances)) continue;
                    scenarioNumber++;
                    runScenario(config, results, skipped, requestedPartitions,
                            actualPartitions, serviceInstances, inputRate, scenarioNumber, totalScenarios);
                }
            }
        }
        reports.write(config.resultsDir(), results, skipped);
        System.out.printf("[benchmark] COMPLETED: %d scenario%s processed, %d measured run%s, "
                        + "%d skipped, elapsed %.1fs%n",
                totalScenarios, totalScenarios == 1 ? "" : "s",
                results.size(), results.size() == 1 ? "" : "s", skipped.size(),
                elapsedSeconds(matrixStarted));
    }

    static boolean shouldSkip(int requested, int actual) { return actual > requested; }
    static boolean isMeaningfulScalingScenario(int partitions, int services) { return services <= partitions; }

    private void runScenario(Config config, List<BenchmarkResult> results, List<SkippedScenario> skipped,
                             int requestedPartitions, int actualPartitions,
                             int serviceInstances, long inputRate, int scenarioNumber,
                             int totalScenarios) throws Exception {
        long scenarioStarted = System.nanoTime();
        String scenario = "[scenario " + scenarioNumber + "/" + totalScenarios + "]";
        System.out.printf("%s Starting: %ds, %d partition%s, %d service%s, %,d events/s%n",
                scenario, config.measurementSeconds(), requestedPartitions,
                requestedPartitions == 1 ? "" : "s", serviceInstances,
                serviceInstances == 1 ? "" : "s", inputRate);
        if (shouldSkip(requestedPartitions, actualPartitions)) {
            String reason = "The retained topics already have " + actualPartitions
                    + " partitions; Kafka partitions cannot be decreased.";
            skipped.add(new SkippedScenario(config.measurementSeconds(), requestedPartitions, actualPartitions,
                    serviceInstances, inputRate, reason));
            reports.write(config.resultsDir(), results, skipped);
            System.out.printf("%s SKIPPED: %s%n", scenario, reason);
            return;
        }

        if (config.warmupSeconds() > 0) {
            long seed = seed(requestedPartitions, serviceInstances, inputRate, 0);
            streams.run(config, config.warmupSeconds(), requestedPartitions, actualPartitions,
                    serviceInstances, inputRate, 0, seed);
            plain.run(config, config.warmupSeconds(), requestedPartitions, actualPartitions,
                    serviceInstances, inputRate, 0, seed);
        }
        for (int iteration = 1; iteration <= config.iterations(); iteration++) {
            long seed = seed(requestedPartitions, serviceInstances, inputRate, iteration);
            results.add(streams.run(config, config.measurementSeconds(), requestedPartitions, actualPartitions,
                    serviceInstances, inputRate, iteration, seed));
            reports.write(config.resultsDir(), results, skipped);
            System.out.printf("[report] Updated: %d measured run%s, %d skipped%n",
                    results.size(), results.size() == 1 ? "" : "s", skipped.size());
            results.add(plain.run(config, config.measurementSeconds(), requestedPartitions, actualPartitions,
                    serviceInstances, inputRate, iteration, seed));
            reports.write(config.resultsDir(), results, skipped);
            System.out.printf("[report] Updated: %d measured run%s, %d skipped%n",
                    results.size(), results.size() == 1 ? "" : "s", skipped.size());
        }
        System.out.printf("%s COMPLETED in %.1fs%n", scenario, elapsedSeconds(scenarioStarted));
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

    private static long seed(int partitions, int serviceInstances, long inputRate, int iteration) {
        return 42L + partitions * 17L + serviceInstances * 13L + inputRate * 7L + iteration;
    }
}
