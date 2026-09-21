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

    BenchmarkResult run(Config config, BenchmarkType benchmarkType, PreparedDataset dataset,
                        int requestedPartitions, int actualPartitions,
                        int serviceInstances, int iteration) throws Exception {
        long runStarted = System.nanoTime();
        String phase = iteration == 0 ? "warm-up" : "iteration " + iteration;
        String prefix = "[" + implementation + "][" + display(benchmarkType) + "][" + phase + "]";
        String runId = RunSupport.runId(implementation);
        String groupId = groupPrefix + runId;
        Generation generation = dataset.generation();
        System.out.printf("%s STARTING: %d service%s, %d partition%s, %,d pre-produced input records%n",
                prefix, serviceInstances, serviceInstances == 1 ? "" : "s",
                actualPartitions, actualPartitions == 1 ? "" : "s", generation.sent());
        KafkaSupport.prepareGroupAtOffsets(config, groupId, config.inputTopic(), dataset.startOffsets());
        if (benchmarkType == BenchmarkType.END_TO_END && config.processingMode() == ProcessingMode.METADATA)
            KafkaSupport.prepareGroupAtEnd(config, groupId, config.partialTopic());

        StageMetrics metrics = new StageMetrics(generation.sent());
        WorkerCommand command = new WorkerCommand(implementation, runId, dataset.workloadId(), groupId,
                benchmarkType, generation.sent(), actualPartitions, dataset.durationSeconds());
        OutputCollector collector = benchmarkType == BenchmarkType.END_TO_END
                ? new OutputCollector(config, runId, metrics) : null;
        try {
            if (collector != null)
                collector.expect(generation.expectedOutputs(), generation.expectedAggregates());
            try (DistributedWorkers workers = DistributedWorkers.start(config, serviceInstances, command)) {
                System.out.printf("%s Waiting for %d stable consumer-group member%s%n", prefix,
                        serviceInstances, serviceInstances == 1 ? "" : "s");
                KafkaSupport.awaitGroupMembers(config, groupId, serviceInstances);
                System.out.printf("%s MEASUREMENT START: releasing workers onto reusable backlog%n", prefix);
                long processingStarted = System.nanoTime();
                workers.resume();
                long drainDeadline = System.nanoTime() + Duration.ofSeconds(config.timeoutSeconds()).toNanos();
                WorkerProgress progress = workers.awaitConsumed(generation.sent(), remaining(drainDeadline));
                boolean inputsCompleted = progress.consumed() >= generation.sent();
                boolean outputsCompleted = collector == null || collector.await(remaining(drainDeadline));
                long finished = System.nanoTime();
                System.out.printf("%s %s: %,d/%,d inputs consumed%s%n", prefix,
                        inputsCompleted && outputsCompleted ? "DRAIN COMPLETE" : "TIMEOUT",
                        progress.consumed(), generation.sent(), collector == null ? ""
                                : String.format(", %,d/%,d outputs observed", collector.observed(), generation.expectedOutputs()));

                List<ProcessorReport> reports = workers.stop();
                reports.forEach(report -> metrics.merge(report.metrics()));
                ProcessorReport aggregate = ProcessorReport.combine(reports);
                Validation validation = collector == null
                        ? ingestionValidation(generation.sent(), aggregate)
                        : collector.validation(generation.sent(), aggregate.consumed(), aggregate.published());
                ActiveProcessing active = activeProcessing(reports, aggregate.consumed());
                BenchmarkResult result = RunSupport.result(config, implementation, benchmarkType, runId, iteration,
                        requestedPartitions, actualPartitions, serviceInstances,
                        reports.stream().map(ProcessorReport::consumed).toList(), generation,
                        metrics, processingStarted, finished, aggregate.resources(), validation,
                        aggregate.gcCount(), aggregate.gcTimeMs(), active.seconds(), active.throughput(), active.meanMs());
                System.out.printf("%s %s: consumed per service %s, validation %s, elapsed %.2fs, wall time %.1fs%n",
                        prefix, validation.valid() ? "COMPLETED" : "INVALID", result.eventsConsumedPerService(),
                        validation.valid() ? "passed" : "failed", result.totalElapsedSeconds(),
                        (System.nanoTime() - runStarted) / 1_000_000_000.0);
                return result;
            }
        } finally {
            if (collector != null) collector.close();
        }
    }

    static ActiveProcessing activeProcessing(List<ProcessorReport> reports, int consumed) {
        long maxWorkerNanos = reports.stream().map(ProcessorReport::metrics)
                .mapToLong(MetricsSnapshot::processingNanos).max().orElse(0);
        long totalNanos = reports.stream().map(ProcessorReport::metrics)
                .mapToLong(MetricsSnapshot::processingNanos).sum();
        long count = reports.stream().map(ProcessorReport::metrics)
                .mapToLong(MetricsSnapshot::processingCount).sum();
        double seconds = maxWorkerNanos / 1_000_000_000.0;
        return new ActiveProcessing(seconds, seconds == 0 ? 0 : consumed / seconds,
                count == 0 ? 0 : totalNanos / (double) count / 1_000_000.0);
    }

    private static Validation ingestionValidation(int sent, ProcessorReport aggregate) {
        return new Validation(0, sent, aggregate.consumed(), 0, 0, 0, 0, 0, 0);
    }

    private static String display(BenchmarkType type) {
        return type == BenchmarkType.END_TO_END ? "end-to-end" : "ingestion-only";
    }

    private static Duration remaining(long deadlineNanos) {
        return Duration.ofNanos(Math.max(0, deadlineNanos - System.nanoTime()));
    }
}

record ActiveProcessing(double seconds, double throughput, double meanMs) {}
