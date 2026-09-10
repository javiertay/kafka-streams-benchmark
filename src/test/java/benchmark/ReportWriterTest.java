package benchmark;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReportWriterTest {
    @Test void highlightsHigherThroughputAndLowerLatencyCpuAndRam() {
        assertTrue(ReportWriter.metricRow("throughput", 20, 10, true, "/s", true)
                .contains("class=\"better\">20.00/s"));
        assertTrue(ReportWriter.metricRow("latency", 5, 10, false, " ms", true)
                .contains("class=\"better\">5.00 ms"));
        assertTrue(ReportWriter.metricRow("CPU", 5, 10, false, "%", true)
                .contains("class=\"better\">5.00%"));
        assertTrue(ReportWriter.metricRow("RAM", 5, 10, false, " MB", true)
                .contains("class=\"better\">5.00 MB"));
    }

    @Test void tiesAndInvalidRunsHaveNoWinner() {
        assertFalse(ReportWriter.metricRow("tie", 1.001, 1.004, true, "", true).contains("better"));
        assertFalse(ReportWriter.metricRow("invalid", 20, 10, true, "", false).contains("better"));
    }

    @Test void generatedHtmlExplainsWhatIsMeasuredAndWhy() {
        String html = new ReportWriter().html(List.of(), List.of());
        assertTrue(html.contains("What performance are we testing?"));
        assertTrue(html.contains("Why are we doing this?"));
        assertTrue(html.contains("Overall summary"));
        assertTrue(html.contains("KafkaConsumer"));
    }

    @Test void summaryNamesWinnerDifferenceTieAndInvalidRun() {
        assertEquals("Kafka Streams had 25.0% higher total processing throughput.", ReportWriter.winnerSummary(100, 80, true));
        assertEquals("Plain Java had 25.0% higher total processing throughput.", ReportWriter.winnerSummary(80, 100, true));
        assertEquals("Tie at the displayed precision.", ReportWriter.winnerSummary(1.001, 1.004, true));
        assertEquals("No winner because output validation failed.", ReportWriter.winnerSummary(100, 80, false));
    }

    @Test void overallSummaryCountsConfigurationWinsAndExcludesInvalidComparisons() {
        List<BenchmarkResult> results = List.of(
                result("Kafka Streams", 100_000, 1, 120, true),
                result("Plain Java", 100_000, 1, 100, true),
                result("Kafka Streams", 1_000_000, 3, 110, true),
                result("Plain Java", 1_000_000, 3, 100, true),
                result("Kafka Streams", 100_000, 6, 90, true),
                result("Plain Java", 100_000, 6, 110, true),
                result("Kafka Streams", 1_000_000, 6, 90, false),
                result("Plain Java", 1_000_000, 6, 110, true));

        assertEquals("Kafka Streams did better overall: 2 configuration wins versus 1 for Plain Java, "
                        + "with 0 ties. 1 invalid comparison excluded.",
                ReportWriter.overallSummary(results));
    }

    @Test void keepsEventCountsAndConsumerCountsInSeparateReportScenarios() {
        String html = new ReportWriter().html(List.of(
                result("Kafka Streams", 100_000, 1), result("Plain Java", 100_000, 1),
                result("Kafka Streams", 1_000_000, 3), result("Plain Java", 1_000_000, 3)), List.of());

        assertTrue(html.contains("100,000 events"));
        assertTrue(html.contains("1,000,000 events"));
        assertTrue(html.contains("1 service"));
        assertTrue(html.contains("3 services"));
        assertTrue(html.contains("3 brokers, replication factor 3"));
        assertTrue(html.contains("only this broker-count environment"));
        assertTrue(html.contains("Metadata deduplication"));
        assertTrue(html.contains("Expected outputs"));
        assertTrue(html.contains("Metadata flush p99"));
        assertTrue(html.contains("Fixed input records"));
        assertTrue(html.contains("Total processing throughput"));
        assertEquals(2, html.split("role=\"tab\"", -1).length - 1);
        assertFalse(html.contains("Generation throughput"));
        assertFalse(html.contains("Catch-up time"));
    }

    private static BenchmarkResult result(String implementation, int eventCount, int services) {
        return result(implementation, eventCount, services, 10, true);
    }

    private static BenchmarkResult result(String implementation, int eventCount, int services,
                                          double totalThroughput, boolean valid) {
        Latency latency = new Latency(1, 2, 3, 4);
        return new BenchmarkResult(implementation, "2026-01-01T00:00:00Z",
                implementation + eventCount + services, 1, eventCount, 3, 3, 256, services,
                java.util.Collections.nCopies(services, eventCount / services),
                1, 10, 1, 10, latency, latency, 1, 10, latency,
                10, totalThroughput, new ResourceUsage(1, 2, 3, 4),
                new Validation(eventCount, eventCount, eventCount, eventCount, valid ? eventCount : eventCount - 1,
                        valid ? 0 : 1, 0, 0, 0),
                new RuntimeDetails("25", "vendor", "vm", "4.1.0", "G1", 512, "", 0, 0),
                Map.of("brokerCount", 3, "replicationFactor", 3, "processingMode", "metadata"));
    }
}
