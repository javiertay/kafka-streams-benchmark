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
        int totalScenarios = config.partitions().stream()
                .mapToInt(partitions -> (int) config.serviceInstances().stream()
                        .filter(services -> isMeaningfulScalingScenario(partitions, services)).count())
                .sum();
        if (config.processingMode() == ProcessingMode.VEHICLE_CONGESTION)
            System.out.printf("[benchmark] Starting matrix: %d scenarios, 2 implementations, %d measured iterations, %ds/run, %ds warm-up, %d jobs at %d fps%n",
                    totalScenarios, config.iterations(), config.durationSeconds(), config.warmupDurationSeconds(),
                    config.simulatedJobs(), config.framesPerSecond());
        else
            System.out.printf("[benchmark] Starting matrix: %d scenario%s, 2 implementations, "
                            + "%d measured iteration%s, %ds/run, %ds warm-up, %,d-%,d inputs/s%n",
                    totalScenarios, totalScenarios == 1 ? "" : "s",
                    config.iterations(), config.iterations() == 1 ? "" : "s",
                    config.durationSeconds(), config.warmupDurationSeconds(),
                    config.minInputsPerSecond(), config.maxInputsPerSecond());
        List<BenchmarkResult> results = new ArrayList<>();
        List<SkippedScenario> skipped = new ArrayList<>();
        int scenarioNumber = 0;
        for (int requestedPartitions : config.partitions()) {
            int actualPartitions = KafkaSupport.ensurePartitions(config, requestedPartitions);
            PreparedDataset warmup = null;
            PreparedDataset measured = null;
            if (!shouldSkip(requestedPartitions, actualPartitions)) {
                if (config.warmupDurationSeconds() > 0) {
                    System.out.printf("[benchmark] Pre-producing %d-partition warm-up dataset (not measured)%n",
                            requestedPartitions);
                    warmup = RunSupport.prepareDataset(config, RunSupport.runId("warmup-workload"),
                            config.warmupDurationSeconds(), config.workloadSeed(), actualPartitions);
                }
                System.out.printf("[benchmark] Pre-producing %d-partition measured dataset (preparation is not measured)%n",
                        requestedPartitions);
                measured = RunSupport.prepareDataset(config, RunSupport.runId("workload"),
                        config.durationSeconds(), config.workloadSeed(), actualPartitions);
            }
            for (int serviceInstances : config.serviceInstances()) {
                if (!isMeaningfulScalingScenario(requestedPartitions, serviceInstances)) continue;
                scenarioNumber++;
                runScenario(config, results, skipped, requestedPartitions,
                        actualPartitions, serviceInstances, scenarioNumber, totalScenarios, warmup, measured);
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
                             int serviceInstances, int scenarioNumber,
                             int totalScenarios, PreparedDataset warmup,
                             PreparedDataset dataset) throws Exception {
        long scenarioStarted = System.nanoTime();
        String scenario = "[scenario " + scenarioNumber + "/" + totalScenarios + "]";
        System.out.printf("%s Starting: %,d events, %d partition%s, %d service%s%n",
                scenario, config.processingMode() == ProcessingMode.VEHICLE_CONGESTION
                        ? VehicleWorkload.eventCount(config, config.durationSeconds())
                        : Workload.inputSchedule(config.durationSeconds(), config.minInputsPerSecond(),
                        config.maxInputsPerSecond(), config.workloadSeed()).stream().mapToInt(Integer::intValue).sum(), requestedPartitions,
                requestedPartitions == 1 ? "" : "s", serviceInstances,
                serviceInstances == 1 ? "" : "s");
        if (shouldSkip(requestedPartitions, actualPartitions)) {
            String reason = "The retained topics already have " + actualPartitions
                    + " partitions; Kafka partitions cannot be decreased.";
            skipped.add(new SkippedScenario(config.processingMode() == ProcessingMode.VEHICLE_CONGESTION
                            ? VehicleWorkload.eventCount(config, config.durationSeconds())
                            : Workload.inputSchedule(config.durationSeconds(), config.minInputsPerSecond(),
                            config.maxInputsPerSecond(), config.workloadSeed()).stream().mapToInt(Integer::intValue).sum(),
                    requestedPartitions, actualPartitions,
                    serviceInstances, reason));
            reports.write(config.resultsDir(), results, skipped);
            System.out.printf("%s SKIPPED: %s%n", scenario, reason);
            return;
        }

        if (warmup != null) {
            runWarmups(config, warmup, requestedPartitions, actualPartitions, serviceInstances);
        }
        if (dataset == null) throw new IllegalStateException("Measured dataset was not prepared");
        for (int iteration = 1; iteration <= config.iterations(); iteration++) {
            DistributedRunner first = streamsFirst(iteration) ? streams : plain;
            DistributedRunner second = streamsFirst(iteration) ? plain : streams;
            for (BenchmarkType type : List.of(BenchmarkType.INGESTION_ONLY, BenchmarkType.END_TO_END)) {
                runAndReport(first, type, dataset, config, results, skipped, requestedPartitions,
                        actualPartitions, serviceInstances, iteration);
                runAndReport(second, type, dataset, config, results, skipped, requestedPartitions,
                        actualPartitions, serviceInstances, iteration);
            }
        }
        System.out.printf("%s COMPLETED in %.1fs%n", scenario, elapsedSeconds(scenarioStarted));
    }

    static boolean streamsFirst(int iteration) { return iteration % 2 == 1; }

    private void runWarmups(Config config, PreparedDataset dataset, int requestedPartitions,
                            int actualPartitions, int serviceInstances) throws Exception {
        for (BenchmarkType type : List.of(BenchmarkType.INGESTION_ONLY, BenchmarkType.END_TO_END)) {
            streams.run(config, type, dataset, requestedPartitions, actualPartitions, serviceInstances, 0);
            plain.run(config, type, dataset, requestedPartitions, actualPartitions, serviceInstances, 0);
        }
    }

    private void runAndReport(DistributedRunner runner, BenchmarkType type, PreparedDataset dataset,
                              Config config, List<BenchmarkResult> results,
                              List<SkippedScenario> skipped, int requestedPartitions, int actualPartitions,
                              int serviceInstances, int iteration) throws Exception {
        results.add(runner.run(config, type, dataset, requestedPartitions, actualPartitions,
                serviceInstances, iteration));
        reports.write(config.resultsDir(), results, skipped);
        System.out.printf("[report] Updated: %d measured run%s, %d skipped%n",
                results.size(), results.size() == 1 ? "" : "s", skipped.size());
    }

    private static double elapsedSeconds(long startedNanos) {
        return (System.nanoTime() - startedNanos) / 1_000_000_000.0;
    }

}
