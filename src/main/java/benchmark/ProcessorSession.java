package benchmark;

import java.util.List;

interface ProcessorSession {
    WorkerProgress progress();
    ProcessorReport stop() throws Exception;

    static ProcessorSession start(Config config, WorkerCommand command) throws Exception {
        StageMetrics metrics = new StageMetrics(command.estimatedEvents());
        return switch (command.implementation()) {
            case "Kafka Streams" -> new KafkaStreamsProcessor(config, command, metrics);
            case "Plain Java" -> new TraditionalKafkaProcessor(config, command, metrics);
            default -> throw new IllegalArgumentException("Unknown implementation: " + command.implementation());
        };
    }
}

record WorkerCommand(String implementation, String runId, String groupId,
                     long estimatedEvents, int inputPartitions, int durationSeconds) {}

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
