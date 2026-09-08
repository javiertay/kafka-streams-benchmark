package benchmark;

import java.util.Map;

record Validation(int expected, int sent, int consumed, int published, int observed,
                  int missing, int duplicates, int unexpected) {
    boolean valid() {
        return expected == observed && missing == 0 && duplicates == 0 && unexpected == 0;
    }
}

record ResourceUsage(double averageCpuPercent, double peakCpuPercent,
                     double averageRamMb, double peakRamMb) {}

record RuntimeDetails(String javaVersion, String javaVendor, String vmName,
                      String kafkaVersion, String gc, long maxHeapMb, String jvmFlags,
                      long gcCount, long gcTimeMs) {}

record BenchmarkResult(
        String implementation, String timestamp, String runId, int iteration,
        int eventCount, int requestedPartitions, int actualPartitions, int payloadBytes,
        int processingThreads, long requestedInputRate, double achievedInputRate,
        double ingestionThroughput, Latency ingestionLatency,
        double processingThroughput, Latency processingLatency,
        double publishingThroughput, Latency publishingLatency,
        double totalElapsedSeconds, double totalThroughput, Latency endToEndLatency,
        ResourceUsage resources, Validation validation, RuntimeDetails technicalDetails,
        Map<String, Object> safeConfiguration) {}

record SkippedScenario(int eventCount, int requestedPartitions, int actualPartitions,
                       int processingThreads, long requestedInputRate, String reason) {}
