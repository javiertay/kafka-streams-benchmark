package benchmark;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

interface ProcessorSession {
    WorkerProgress progress();
    void resume();
    ProcessorReport stop() throws Exception;

    static ProcessorSession start(Config config, WorkerCommand command) throws Exception {
        StageMetrics metrics = new StageMetrics(command.estimatedEvents());
        ProcessingGate gate = new ProcessingGate();
        return switch (command.implementation()) {
            case "Kafka Streams" -> new KafkaStreamsProcessor(config, command, metrics, gate);
            case "Plain Java" -> new TraditionalKafkaProcessor(config, command, metrics, gate);
            default -> throw new IllegalArgumentException("Unknown implementation: " + command.implementation());
        };
    }
}

final class ProcessingGate {
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicBoolean open = new AtomicBoolean();

    void await() {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for benchmark start", exception);
        }
    }

    void open() {
        open.set(true);
        latch.countDown();
    }

    boolean isOpen() { return open.get(); }
}

enum BenchmarkType { END_TO_END, INGESTION_ONLY }

record WorkerCommand(String implementation, String runId, String workloadId, String groupId,
                     BenchmarkType benchmarkType, long estimatedEvents,
                     int inputPartitions, int durationSeconds) {}

record WorkerProgress(String runId, int consumed, int published) {
    static WorkerProgress combine(String runId, List<WorkerProgress> progress) {
        return new WorkerProgress(runId,
                progress.stream().mapToInt(WorkerProgress::consumed).sum(),
                progress.stream().mapToInt(WorkerProgress::published).sum());
    }
}

record ProcessorReport(int consumed, int published, MetricsSnapshot metrics, ResourceUsage resources,
                       List<ResourceSample> resourceSamples, long gcCount, long gcTimeMs) {
    static ProcessorReport combine(List<ProcessorReport> reports) {
        return new ProcessorReport(reports.stream().mapToInt(ProcessorReport::consumed).sum(),
                reports.stream().mapToInt(ProcessorReport::published).sum(), null,
                ResourceSampler.aggregate(reports), List.of(),
                reports.stream().mapToLong(ProcessorReport::gcCount).sum(),
                reports.stream().mapToLong(ProcessorReport::gcTimeMs).sum());
    }
}
