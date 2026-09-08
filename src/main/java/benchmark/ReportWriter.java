package benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

final class ReportWriter {
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules().enable(SerializationFeature.INDENT_OUTPUT);

    void write(Path directory, List<BenchmarkResult> results, List<SkippedScenario> skipped) throws IOException {
        Files.createDirectories(directory);
        for (BenchmarkResult result : results) {
            Files.writeString(directory.resolve("raw-" + result.runId() + ".json"), JSON.writeValueAsString(result));
        }
        Files.writeString(directory.resolve("summary.json"), JSON.writeValueAsString(new Summary(results, skipped)));
        Files.writeString(directory.resolve("index.html"), html(results, skipped), StandardCharsets.UTF_8);
    }

    String html(List<BenchmarkResult> results, List<SkippedScenario> skipped) {
        StringBuilder scenarios = new StringBuilder();
        results.stream().map(result -> new Scenario(result.eventCount(), result.requestedPartitions(), result.actualPartitions()))
                .distinct().sorted(Comparator.comparingInt(Scenario::partitions).thenComparingInt(Scenario::events))
                .forEach(scenario -> scenarios.append(comparison(scenario, results)));
        StringBuilder skipHtml = new StringBuilder();
        skipped.forEach(skip -> skipHtml.append("<li><strong>").append(number(skip.eventCount())).append(" events, ")
                .append(skip.requestedPartitions()).append(" requested partitions:</strong> ")
                .append(escape(skip.reason())).append("</li>"));
        return """
                <!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
                <title>Kafka Streams vs Plain Java Benchmark</title><style>
                :root{color-scheme:light;font-family:system-ui,sans-serif;color:#172033;background:#f4f7fb}body{margin:0}main{max-width:1180px;margin:auto;padding:2rem}
                header,.card{background:#fff;border:1px solid #dce3ed;border-radius:14px;padding:1.4rem;margin-bottom:1rem;box-shadow:0 3px 14px #24334a0d}
                h1{margin-top:0}.lead{font-size:1.1rem;line-height:1.55;max-width:78ch}.purpose{border-left:5px solid #3157c8;padding-left:1rem}
                table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{text-align:right;padding:.65rem;border-bottom:1px solid #e5eaf1}th:first-child,td:first-child{text-align:left}
                .better{background:#d9f6e5;color:#126234;font-weight:700}.invalid{background:#fff3cd;color:#664d03;padding:.7rem;border-radius:8px}.valid{color:#126234}small,.muted{color:#596579}
                details{margin-top:1rem}code{background:#edf1f7;padding:.1rem .3rem;border-radius:4px}@media(max-width:700px){main{padding:.7rem}.card{overflow-x:auto}th,td{white-space:nowrap}}
                </style></head><body><main>
                <header><h1>Kafka Streams vs Plain Java</h1><p class="lead purpose"><strong>What performance are we testing?</strong> This benchmark measures how quickly each implementation ingests, processes, and publishes the same deterministic JSON events, plus total throughput, end-to-end latency, CPU, and RAM.</p>
                <p class="lead"><strong>Why are we doing this?</strong> To make an evidence-based choice between Kafka Streams and direct <code>KafkaConsumer</code>/<code>KafkaProducer</code> code under the same Java 25 runtime, Kafka cluster, workload, partitions, processing logic, and resource limits. Results describe this environment only; they are not universal performance claims.</p>
                <p class="muted">Green marks the better displayed value. Values that round to the same display precision are ties. Invalid runs never receive a winner.</p></header>
                <section class="card"><h2>How to read the results</h2><p><strong>Ingestion:</strong> how quickly the application receives events from Kafka. <strong>Processing:</strong> how quickly it handles each received event. <strong>Publishing:</strong> how long a processed event takes to reach and be observed on Kafka output (a comparable publish-to-observe measurement, not producer API submission time). <strong>Total throughput:</strong> complete observed events per second. <strong>End-to-end latency:</strong> total travel time through generate → ingest → process → publish → observe. <strong>CPU/RAM:</strong> processor capacity and memory used by this benchmark process.</p></section>
                """ + scenarios + (skipHtml.isEmpty() ? "" : "<section class=\"card\"><h2>Skipped scenarios</h2><ul>" + skipHtml + "</ul></section>") + """
                <section class="card"><h2>Advanced JVM metrics</h2><p>Runtime, GC, heap, and safe Kafka configuration are available in <a href="summary.json">summary.json</a> and the per-run raw JSON files. Credentials are never written.</p>
                <details><summary>Measurement notes and limitations</summary><p>Kafka Streams does not expose a per-record producer acknowledgement callback. For fairness, publishing latency for both implementations is measured from processing completion until the output observer receives the record. CPU and RAM are sampled for the whole single Java process. Historical records are filtered by unique run ID.</p></details></section>
                </main></body></html>
                """;
    }

    private String comparison(Scenario scenario, List<BenchmarkResult> all) {
        List<BenchmarkResult> streamsRuns = matching(all, scenario, "Kafka Streams");
        List<BenchmarkResult> plainRuns = matching(all, scenario, "Plain Java");
        if (streamsRuns.isEmpty() || plainRuns.isEmpty()) return "";
        BenchmarkResult streams = Statistics.medianBy(streamsRuns, BenchmarkResult::totalThroughput);
        BenchmarkResult plain = Statistics.medianBy(plainRuns, BenchmarkResult::totalThroughput);
        boolean valid = streamsRuns.stream().allMatch(r -> r.validation().valid()) && plainRuns.stream().allMatch(r -> r.validation().valid());
        StringBuilder rows = new StringBuilder();
        rows.append(metricRow("Ingestion throughput", streams.ingestionThroughput(), plain.ingestionThroughput(), true, "/s", valid));
        rows.append(metricRow("Ingestion p50", streams.ingestionLatency().p50Ms(), plain.ingestionLatency().p50Ms(), false, " ms", valid));
        rows.append(metricRow("Ingestion p95", streams.ingestionLatency().p95Ms(), plain.ingestionLatency().p95Ms(), false, " ms", valid));
        rows.append(metricRow("Ingestion p99", streams.ingestionLatency().p99Ms(), plain.ingestionLatency().p99Ms(), false, " ms", valid));
        rows.append(metricRow("Processing throughput", streams.processingThroughput(), plain.processingThroughput(), true, "/s", valid));
        rows.append(metricRow("Processing p99", streams.processingLatency().p99Ms(), plain.processingLatency().p99Ms(), false, " ms", valid));
        rows.append(metricRow("Publishing throughput", streams.publishingThroughput(), plain.publishingThroughput(), true, "/s", valid));
        rows.append(metricRow("Publishing p99", streams.publishingLatency().p99Ms(), plain.publishingLatency().p99Ms(), false, " ms", valid));
        rows.append(metricRow("Total elapsed time", streams.totalElapsedSeconds(), plain.totalElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Total throughput", streams.totalThroughput(), plain.totalThroughput(), true, "/s", valid));
        rows.append(metricRow("End-to-end p50", streams.endToEndLatency().p50Ms(), plain.endToEndLatency().p50Ms(), false, " ms", valid));
        rows.append(metricRow("End-to-end p95", streams.endToEndLatency().p95Ms(), plain.endToEndLatency().p95Ms(), false, " ms", valid));
        rows.append(metricRow("End-to-end p99", streams.endToEndLatency().p99Ms(), plain.endToEndLatency().p99Ms(), false, " ms", valid));
        rows.append(metricRow("Average CPU", streams.resources().averageCpuPercent(), plain.resources().averageCpuPercent(), false, "%", valid));
        rows.append(metricRow("Peak CPU", streams.resources().peakCpuPercent(), plain.resources().peakCpuPercent(), false, "%", valid));
        rows.append(metricRow("Average RAM", streams.resources().averageRamMb(), plain.resources().averageRamMb(), false, " MB", valid));
        rows.append(metricRow("Peak RAM", streams.resources().peakRamMb(), plain.resources().peakRamMb(), false, " MB", valid));
        String range = String.format(Locale.ROOT, "Median of %d/%d iterations; total throughput ranges %.1f–%.1f/s vs %.1f–%.1f/s.",
                streamsRuns.size(), plainRuns.size(), min(streamsRuns), max(streamsRuns), min(plainRuns), max(plainRuns));
        String status = valid ? "<span class=\"valid\">Valid: all expected outputs were observed once.</span>" :
                "<div class=\"invalid\">Invalid: output validation failed. No winner is highlighted.</div>";
        return "<section class=\"card\"><h2>" + number(scenario.events()) + " events · " + scenario.actual() + " partitions</h2><p>" + status + "</p><p><strong>Overall result:</strong> " + winnerSummary(streams.totalThroughput(), plain.totalThroughput(), valid) + "</p><p class=\"muted\">" + range + "</p><table><thead><tr><th>Metric</th><th>Kafka Streams</th><th>Plain Java</th></tr></thead><tbody>" + rows + "</tbody></table></section>";
    }

    static String winnerSummary(double streams, double plain, boolean valid) {
        if (!valid) return "No winner because output validation failed.";
        if (rounded(streams) == rounded(plain)) return "Tie at the displayed precision.";
        double slower = Math.min(streams, plain);
        double difference = slower == 0 ? 0 : (Math.max(streams, plain) - slower) / slower * 100;
        String winner = streams > plain ? "Kafka Streams" : "Plain Java";
        return String.format(Locale.ROOT, "%s had %.1f%% higher total throughput.", winner, difference);
    }

    static String metricRow(String label, double left, double right, boolean higherIsBetter, String suffix, boolean valid) {
        String leftClass = "", rightClass = "";
        if (valid && rounded(left) != rounded(right)) {
            boolean leftWins = higherIsBetter ? left > right : left < right;
            leftClass = leftWins ? " class=\"better\"" : "";
            rightClass = leftWins ? "" : " class=\"better\"";
        }
        return String.format(Locale.ROOT, "<tr><td>%s</td><td%s>%,.2f%s</td><td%s>%,.2f%s</td></tr>",
                escape(label), leftClass, left, suffix, rightClass, right, suffix);
    }

    private static long rounded(double value) { return Math.round(value * 100); }
    private static List<BenchmarkResult> matching(List<BenchmarkResult> all, Scenario scenario, String implementation) {
        return all.stream().filter(r -> r.eventCount() == scenario.events() && r.requestedPartitions() == scenario.partitions()
                && r.implementation().equals(implementation)).toList();
    }
    private static double min(List<BenchmarkResult> values) { return values.stream().mapToDouble(BenchmarkResult::totalThroughput).min().orElse(0); }
    private static double max(List<BenchmarkResult> values) { return values.stream().mapToDouble(BenchmarkResult::totalThroughput).max().orElse(0); }
    private static String number(int number) { return String.format(Locale.ROOT, "%,d", number); }
    private static String escape(String value) { return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;"); }

    private record Scenario(int events, int partitions, int actual) {}
    private record Summary(List<BenchmarkResult> results, List<SkippedScenario> skippedScenarios) {}
}
