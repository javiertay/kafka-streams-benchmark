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

    BenchmarkResult run(Config config, int measurementSeconds, int requestedPartitions, int actualPartitions,
                        int serviceInstances, long inputRate, int iteration, long seed) throws Exception {
        long runStarted = System.nanoTime();
        String phase = iteration == 0 ? "warm-up" : "iteration " + iteration;
        String prefix = "[" + implementation + "][" + phase + "]";
        String runId = RunSupport.runId(implementation);
        String groupId = groupPrefix + runId;
        long estimatedEvents = inputRate * measurementSeconds;
        System.out.printf("%s STARTING: %d service%s, %d partition%s, %ds at %,d events/s%n",
                prefix, serviceInstances, serviceInstances == 1 ? "" : "s",
                actualPartitions, actualPartitions == 1 ? "" : "s",
                measurementSeconds, inputRate);
        System.out.printf("%s Preparing consumer group at the input-topic end%n", prefix);
        KafkaSupport.prepareGroupAtEnd(config, groupId, config.inputTopic());
        StageMetrics metrics = new StageMetrics(estimatedEvents);
        WorkerCommand command = new WorkerCommand(implementation, runId, groupId, estimatedEvents);
        System.out.printf("%s Starting %d worker service%s and output observer%n", prefix,
                serviceInstances, serviceInstances == 1 ? "" : "s");
        try (OutputCollector collector = new OutputCollector(config, runId, metrics);
             DistributedWorkers workers = DistributedWorkers.start(config, serviceInstances, command)) {
            System.out.printf("%s Waiting for %d consumer-group member%s%n", prefix,
                    serviceInstances, serviceInstances == 1 ? "" : "s");
            KafkaSupport.awaitGroupMembers(config, groupId, serviceInstances);
            System.out.printf("%s READY: all workers joined; generating for %ds at %,d events/s%n",
                    prefix, measurementSeconds, inputRate);
            Generation generation = RunSupport.generate(config, runId, measurementSeconds, inputRate, seed,
                    collector::observed);
            int backlogAtGenerationEnd = Math.max(0, generation.sent() - generation.observedAtWindowEnd());
            System.out.printf("%s Generation window complete: %,d sent, %,.0f events/s achieved, "
                            + "%,d backlog, producer flush %.2fs%n",
                    prefix, generation.sent(), generation.achievedRate(), backlogAtGenerationEnd,
                    generation.producerFlushSeconds());
            collector.expect(generation.sent());
            System.out.printf("%s Draining output: %,d/%,d observed (timeout %ds)%n",
                    prefix, collector.observed(), generation.sent(), config.timeoutSeconds());
            boolean completed = collector.await(Duration.ofSeconds(config.timeoutSeconds()));
            long finished = System.nanoTime();
            double catchUpSeconds = Math.max(0,
                    (finished - generation.windowEndedNanos()) / 1_000_000_000.0);
            if (completed) {
                System.out.printf("%s Drain complete: %,d/%,d observed, catch-up %.2fs%n",
                        prefix, collector.observed(), generation.sent(), catchUpSeconds);
            } else {
                System.err.printf("%s TIMEOUT: %,d/%,d observed after %ds%n",
                        prefix, collector.observed(), generation.sent(), config.timeoutSeconds());
            }
            System.out.printf("%s Stopping workers and collecting service metrics%n", prefix);
            List<ProcessorReport> reports = workers.stop();
            reports.forEach(report -> metrics.merge(report.metrics()));
            ProcessorReport aggregate = ProcessorReport.combine(reports);
            Validation validation = collector.validation(generation.sent(), aggregate.consumed(), aggregate.published());
            BenchmarkResult result = RunSupport.result(config, implementation, runId, iteration, measurementSeconds,
                    requestedPartitions, actualPartitions, serviceInstances,
                    reports.stream().map(ProcessorReport::consumed).toList(), inputRate, generation,
                    backlogAtGenerationEnd, metrics, finished, aggregate.resources(), validation,
                    aggregate.gcCount(), aggregate.gcTimeMs());
            System.out.printf("%s %s: consumed per service %s, validation %s, elapsed %.1fs%n",
                    prefix, validation.valid() ? "COMPLETED" : "INVALID",
                    result.eventsConsumedPerService(), validation.valid() ? "passed" : "failed",
                    (System.nanoTime() - runStarted) / 1_000_000_000.0);
            return result;
        }
    }
}
