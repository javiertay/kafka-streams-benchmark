package benchmark;

import java.time.Duration;
import java.util.List;

final class DistributedRunner {
    private final String implementation;
    private final String groupPrefix;

    DistributedRunner(String implementation, String groupPrefix) {
        this.implementation = implementation;
        this.groupPrefix = groupPrefix;
    }

    BenchmarkResult run(Config config, int durationSeconds, int requestedPartitions, int actualPartitions,
                        int serviceInstances, int iteration, long seed) throws Exception {
        long runStarted = System.nanoTime();
        String phase = iteration == 0 ? "warm-up" : "iteration " + iteration;
        String prefix = "[" + implementation + "][" + phase + "]";
        String runId = RunSupport.runId(implementation);
        String groupId = groupPrefix + runId;
        System.out.printf("%s STARTING: %d service%s, %d partition%s, %,d scheduled input events over %ds%n",
                prefix, serviceInstances, serviceInstances == 1 ? "" : "s",
                actualPartitions, actualPartitions == 1 ? "" : "s",
                Workload.inputSchedule(durationSeconds, config.minInputsPerSecond(),
                        config.maxInputsPerSecond(), seed).stream().mapToInt(Integer::intValue).sum(),
                durationSeconds);
        System.out.printf("%s Preparing consumer group at the input-topic end%n", prefix);
        KafkaSupport.prepareGroupAtEnd(config, groupId, config.inputTopic());
        if (config.processingMode() == ProcessingMode.METADATA) {
            KafkaSupport.prepareGroupAtEnd(config, groupId, config.partialTopic());
        }
        int estimatedEvents = Workload.inputSchedule(durationSeconds, config.minInputsPerSecond(),
                config.maxInputsPerSecond(), seed).stream().mapToInt(Integer::intValue).sum();
        StageMetrics metrics = new StageMetrics(estimatedEvents);
        WorkerCommand command = new WorkerCommand(implementation, runId, groupId,
                estimatedEvents, actualPartitions, durationSeconds);
        System.out.printf("%s Starting output observer%n", prefix);
        try (OutputCollector collector = new OutputCollector(config, runId, metrics)) {
            System.out.printf("%s Launching %d worker service%s%n", prefix,
                    serviceInstances, serviceInstances == 1 ? "" : "s");
            try (DistributedWorkers workers = DistributedWorkers.start(config, serviceInstances, command)) {
                System.out.printf("%s Waiting for %d consumer-group member%s%n", prefix,
                        serviceInstances, serviceInstances == 1 ? "" : "s");
                KafkaSupport.awaitGroupMembers(config, groupId, serviceInstances);
                System.out.printf("%s MEASUREMENT START: streaming variable input for %ds%n",
                        prefix, durationSeconds);
                long processingStarted = System.nanoTime();
                Generation generation = RunSupport.generate(config, runId, durationSeconds, seed, actualPartitions);
                collector.expect(generation.expectedOutputs(), generation.expectedAggregates());
                System.out.printf("%s Input complete: %,d records; draining %,d expected outputs (timeout %ds)%n",
                        prefix, generation.sent(), generation.expectedOutputs(), config.timeoutSeconds());
                boolean completed = collector.await(Duration.ofSeconds(config.timeoutSeconds()));
                long finished = System.nanoTime();
                if (completed) {
                    System.out.printf("%s Processing complete: %,d/%,d outputs observed%n",
                            prefix, collector.observed(), generation.expectedOutputs());
                } else {
                    System.err.printf("%s TIMEOUT: %,d/%,d observed after %ds%n",
                            prefix, collector.observed(), generation.expectedOutputs(), config.timeoutSeconds());
                }
                System.out.printf("%s Stopping workers and collecting service metrics%n", prefix);
                List<ProcessorReport> reports = workers.stop();
                reports.forEach(report -> metrics.merge(report.metrics()));
                ProcessorReport aggregate = ProcessorReport.combine(reports);
                Validation validation = collector.validation(
                        generation.sent(), aggregate.consumed(), aggregate.published());
                BenchmarkResult result = RunSupport.result(config, implementation, runId, iteration,
                        requestedPartitions, actualPartitions, serviceInstances,
                        reports.stream().map(ProcessorReport::consumed).toList(), generation,
                        metrics, processingStarted, finished, aggregate.resources(), validation,
                        aggregate.gcCount(), aggregate.gcTimeMs());
                System.out.printf("%s %s: consumed per service %s, validation %s, "
                                + "measured processing %.2fs, wall time %.1fs%n",
                        prefix, validation.valid() ? "COMPLETED" : "INVALID",
                        result.eventsConsumedPerService(), validation.valid() ? "passed" : "failed",
                        result.totalElapsedSeconds(),
                        (System.nanoTime() - runStarted) / 1_000_000_000.0);
                return result;
            }
        }
    }
}
