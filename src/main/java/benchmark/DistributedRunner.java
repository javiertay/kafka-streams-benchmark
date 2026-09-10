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

    BenchmarkResult run(Config config, int eventCount, int requestedPartitions, int actualPartitions,
                        int serviceInstances, int iteration, long seed) throws Exception {
        long runStarted = System.nanoTime();
        String phase = iteration == 0 ? "warm-up" : "iteration " + iteration;
        String prefix = "[" + implementation + "][" + phase + "]";
        String runId = RunSupport.runId(implementation);
        String groupId = groupPrefix + runId;
        System.out.printf("%s STARTING: %d service%s, %d partition%s, %,d fixed input events%n",
                prefix, serviceInstances, serviceInstances == 1 ? "" : "s",
                actualPartitions, actualPartitions == 1 ? "" : "s",
                eventCount);
        System.out.printf("%s Preparing consumer group at the input-topic end%n", prefix);
        KafkaSupport.prepareGroupAtEnd(config, groupId, config.inputTopic());
        System.out.printf("%s Preloading %,d JSON input records (excluded from measurements)%n",
                prefix, eventCount);
        Generation generation = RunSupport.generate(config, runId, eventCount, seed, actualPartitions);
        System.out.printf("%s Input ready: %,d records, %,d expected outputs%n",
                prefix, generation.sent(), generation.expectedOutputs());
        StageMetrics metrics = new StageMetrics(eventCount);
        WorkerCommand command = new WorkerCommand(implementation, runId, groupId, eventCount);
        System.out.printf("%s Starting output observer%n", prefix);
        try (OutputCollector collector = new OutputCollector(config, runId, metrics)) {
            collector.expect(generation.expectedOutputs(), generation.expectedAggregates());
            System.out.printf("%s MEASUREMENT START: launching %d worker service%s%n", prefix,
                    serviceInstances, serviceInstances == 1 ? "" : "s");
            long processingStarted = System.nanoTime();
            try (DistributedWorkers workers = DistributedWorkers.start(config, serviceInstances, command)) {
                System.out.printf("%s Waiting for %d consumer-group member%s%n", prefix,
                        serviceInstances, serviceInstances == 1 ? "" : "s");
                KafkaSupport.awaitGroupMembers(config, groupId, serviceInstances);
                System.out.printf("%s Processing fixed input: %,d/%,d outputs observed (timeout %ds)%n",
                        prefix, collector.observed(), generation.expectedOutputs(), config.timeoutSeconds());
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
