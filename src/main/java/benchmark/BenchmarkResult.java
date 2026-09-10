package benchmark;

import java.util.Map;

record Validation(int expected, int sent, int consumed, int published, int observed,
                  int missing, int duplicates, int unexpected, int incorrect) {
    boolean valid() {
        return sent == consumed && expected == published && expected == observed
                && missing == 0 && duplicates == 0 && unexpected == 0 && incorrect == 0;
    }
}

record ResourceUsage(double averageCpuPercent, double peakCpuPercent,
                     double averageRamMb, double peakRamMb) {}

record RuntimeDetails(String javaVersion, String javaVendor, String vmName,
                      String kafkaVersion, String gc, long maxHeapMb, String jvmFlags,
                      long gcCount, long gcTimeMs) {}

record BenchmarkResult(
        String implementation, String timestamp, String runId, int iteration,
        int eventCount, int durationSeconds, int outputIntervalSeconds,
        int requestedPartitions, int actualPartitions, int payloadBytes,
        int serviceInstances, java.util.List<Integer> eventsConsumedPerService,
        double ingestionElapsedSeconds, double ingestionThroughput,
        double processingElapsedSeconds, double processingThroughput, Latency processingLatency,
        Latency metadataFlushLatency,
        double publishingElapsedSeconds, double publishingThroughput, Latency publishingLatency,
        double totalElapsedSeconds, double totalThroughput,
        ResourceUsage resources, Validation validation, RuntimeDetails technicalDetails,
        Map<String, Object> safeConfiguration) {}

record SkippedScenario(int eventCount, int requestedPartitions, int actualPartitions,
                       int serviceInstances, String reason) {}
