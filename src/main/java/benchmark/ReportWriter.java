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
        String environment = environmentSummary(results);
        StringBuilder scenarios = new StringBuilder();
        List<Scenario> scenarioList = scenarios(results);
        StringBuilder tabs = new StringBuilder();
        for (int index = 0; index < scenarioList.size(); index++) {
            Scenario scenario = scenarioList.get(index);
            tabs.append("<button class=\"config-tab\" role=\"tab\" aria-controls=\"scenario-")
                    .append(index).append("\" aria-selected=\"").append(index == 0).append("\" onclick=\"showScenario(")
                    .append(index).append(")\">").append(number(scenario.eventCount())).append(" events · ")
                    .append(scenario.actual()).append("p · ")
                    .append(services(scenario.serviceInstances())).append("</button>");
            scenarios.append(comparison(scenario, results, index, index == 0));
        }
        StringBuilder skipHtml = new StringBuilder();
        skipped.forEach(skip -> skipHtml.append("<li><strong>").append(number(skip.eventCount())).append(" events, ")
                .append(skip.requestedPartitions()).append(" requested partitions, ")
                .append(services(skip.serviceInstances())).append(":</strong> ")
                .append(escape(skip.reason())).append("</li>"));
        return """
                <!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
                <title>Kafka Streams vs Plain Java Benchmark</title><style>
                :root{color-scheme:light;font-family:system-ui,sans-serif;color:#172033;background:#f4f7fb}body{margin:0}main{max-width:1180px;margin:auto;padding:2rem}
                header,.card{background:#fff;border:1px solid #dce3ed;border-radius:14px;padding:1.4rem;margin-bottom:1rem;box-shadow:0 3px 14px #24334a0d}
                h1{margin-top:0}.lead,.verdict{font-size:1.1rem;line-height:1.55;max-width:78ch}.purpose{border-left:5px solid #3157c8;padding-left:1rem}
                table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{text-align:right;padding:.65rem;border-bottom:1px solid #e5eaf1}th:first-child,td:first-child{text-align:left}
                .better{background:#d9f6e5;color:#126234;font-weight:700}.invalid{background:#fff3cd;color:#664d03;padding:.7rem;border-radius:8px}.valid{color:#126234}small,.muted{color:#596579}
                details{margin-top:1rem}code{background:#edf1f7;padding:.1rem .3rem;border-radius:4px}@media(max-width:700px){main{padding:.7rem}.card{overflow-x:auto}th,td{white-space:nowrap}}
                .config-tabs{display:flex;gap:.5rem;overflow-x:auto;padding:.25rem 0 1rem}.config-tab{white-space:nowrap;border:1px solid #aeb9ca;border-radius:999px;background:#fff;padding:.6rem .9rem;cursor:pointer}.config-tab[aria-selected=true]{background:#3157c8;color:#fff;border-color:#3157c8}.scenario-panel[hidden]{display:none}
                </style></head><body><main>
                <header><h1>Kafka Streams vs Plain Java</h1><p class="lead purpose"><strong>What performance are we testing?</strong> This benchmark streams the same seeded, second-by-second variable input schedule through both implementations. Metadata mode deduplicates track IDs and emits one consolidated payload at each configured interval.</p>
                <p class="lead"><strong>Why are we doing this?</strong> To make an evidence-based choice between Kafka Streams and direct <code>KafkaConsumer</code>/<code>KafkaProducer</code> code under the same Java 25 runtime, Kafka cluster, workload, partitions, processing logic, and resource limits. Results describe this environment only; they are not universal performance claims.</p>
                """ + environment + """
                <p class="muted">Green marks the better displayed value. Values that round to the same display precision are ties. Invalid runs never receive a winner.</p></header>
                <section class="card"><h2>Overall summary</h2><p class="verdict"><strong>""" + escape(overallSummary(results)) + """
                </strong></p><p class="muted">Each complete, valid configuration gets one vote based on median total processing throughput. Workloads are not averaged together.</p></section>
                <section class="card"><h2>How to read the results</h2><p><strong>Scheduled input records:</strong> the total produced by the deterministic variable-rate schedule. <strong>Expected outputs:</strong> equals input records in transform mode; in metadata mode it is one consolidated payload per output interval. <strong>Total elapsed time:</strong> input streaming start until all expected outputs are observed. <strong>CPU/RAM:</strong> totals across processor services only; generator and observer are excluded.</p></section>
                """ + (tabs.isEmpty() ? "" : "<nav class=\"config-tabs\" role=\"tablist\" aria-label=\"Benchmark configurations\">" + tabs + "</nav>")
                + scenarios + (skipHtml.isEmpty() ? "" : "<section class=\"card\"><h2>Skipped scenarios</h2><ul>" + skipHtml + "</ul></section>") + """
                <section class="card"><h2>Advanced JVM metrics</h2><p>Runtime, GC, heap, and safe Kafka configuration are available in <a href="summary.json">summary.json</a> and the per-run raw JSON files. Credentials are never written.</p>
                <details><summary>Measurement notes and limitations</summary><p>Both implementations replay the same seeded rate schedule. Processing is timed identically from JSON decoding through business logic and output handoff. Metadata flush latency includes partition-local payload creation and the global merge. Kafka Streams does not expose a per-record producer acknowledgement callback, so publishing latency is measured from business-logic completion until the output observer receives the record. Latency percentiles use at most 100,000 evenly spaced samples per stage. Historical records are filtered by unique run ID.</p></details></section>
                </main><script>function showScenario(n){document.querySelectorAll('.scenario-panel').forEach((p,i)=>p.hidden=i!==n);document.querySelectorAll('.config-tab').forEach((b,i)=>b.setAttribute('aria-selected',i===n))}</script></body></html>
                """;
    }

    private String comparison(Scenario scenario, List<BenchmarkResult> all, int index, boolean visible) {
        List<BenchmarkResult> streamsRuns = matching(all, scenario, "Kafka Streams");
        List<BenchmarkResult> plainRuns = matching(all, scenario, "Plain Java");
        if (streamsRuns.isEmpty() || plainRuns.isEmpty()) return "";
        BenchmarkResult streams = Statistics.medianBy(streamsRuns, BenchmarkResult::totalThroughput);
        BenchmarkResult plain = Statistics.medianBy(plainRuns, BenchmarkResult::totalThroughput);
        boolean valid = streamsRuns.stream().allMatch(r -> r.validation().valid())
                && plainRuns.stream().allMatch(r -> r.validation().valid());
        StringBuilder rows = new StringBuilder();
        rows.append(neutralRow("Scheduled input records", streams.eventCount(), plain.eventCount(), ""));
        rows.append(neutralRow("Run duration", streams.durationSeconds(), plain.durationSeconds(), " s"));
        rows.append(neutralRow("Output interval", streams.outputIntervalSeconds(), plain.outputIntervalSeconds(), " s"));
        rows.append(neutralRow("Expected outputs", streams.validation().expected(), plain.validation().expected(), ""));
        rows.append(metricRow("Ingestion elapsed time", streams.ingestionElapsedSeconds(), plain.ingestionElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Ingestion throughput", streams.ingestionThroughput(), plain.ingestionThroughput(), true, "/s", valid));
        rows.append(metricRow("Processing elapsed time", streams.processingElapsedSeconds(), plain.processingElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Processing throughput", streams.processingThroughput(), plain.processingThroughput(), true, "/s", valid));
        rows.append(metricRow("Processing p50", streams.processingLatency().p50Ms(), plain.processingLatency().p50Ms(), false, " ms", valid));
        rows.append(metricRow("Processing p95", streams.processingLatency().p95Ms(), plain.processingLatency().p95Ms(), false, " ms", valid));
        rows.append(metricRow("Processing p99", streams.processingLatency().p99Ms(), plain.processingLatency().p99Ms(), false, " ms", valid));
        if (isMetadata(streams)) {
            rows.append(metricRow("Metadata flush p50", streams.metadataFlushLatency().p50Ms(), plain.metadataFlushLatency().p50Ms(), false, " ms", valid));
            rows.append(metricRow("Metadata flush p95", streams.metadataFlushLatency().p95Ms(), plain.metadataFlushLatency().p95Ms(), false, " ms", valid));
            rows.append(metricRow("Metadata flush p99", streams.metadataFlushLatency().p99Ms(), plain.metadataFlushLatency().p99Ms(), false, " ms", valid));
        }
        rows.append(metricRow("Publishing elapsed time", streams.publishingElapsedSeconds(), plain.publishingElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Publishing throughput", streams.publishingThroughput(), plain.publishingThroughput(), true, "/s", valid));
        rows.append(metricRow("Publishing p99", streams.publishingLatency().p99Ms(), plain.publishingLatency().p99Ms(), false, " ms", valid));
        rows.append(metricRow("Total elapsed time", streams.totalElapsedSeconds(), plain.totalElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Total processing throughput", streams.totalThroughput(), plain.totalThroughput(), true, "/s", valid));
        rows.append(metricRow("Average CPU", streams.resources().averageCpuPercent(), plain.resources().averageCpuPercent(), false, "%", valid));
        rows.append(metricRow("Peak CPU", streams.resources().peakCpuPercent(), plain.resources().peakCpuPercent(), false, "%", valid));
        rows.append(metricRow("Average RAM", streams.resources().averageRamMb(), plain.resources().averageRamMb(), false, " MB", valid));
        rows.append(metricRow("Peak RAM", streams.resources().peakRamMb(), plain.resources().peakRamMb(), false, " MB", valid));
        String range = String.format(Locale.ROOT, "Median of %d/%d iterations; total processing throughput ranges %.1f–%.1f/s vs %.1f–%.1f/s.",
                streamsRuns.size(), plainRuns.size(), min(streamsRuns), max(streamsRuns), min(plainRuns), max(plainRuns));
        String status = valid ? "<span class=\"valid\">Valid: both implementations processed the same deterministic input schedule and output validation passed.</span>"
                : "<div class=\"invalid\">Invalid: output validation failed. No winner is highlighted.</div>";
        return "<section class=\"card scenario-panel\" id=\"scenario-" + index + "\" role=\"tabpanel\""
                + (visible ? "" : " hidden") + "><h2>" + number(scenario.eventCount()) + " events · "
                + scenario.actual() + " partitions · " + services(scenario.serviceInstances()) + "</h2><p>"
                + status + "</p><p><strong>Overall result:</strong> "
                + winnerSummary(streams.totalThroughput(), plain.totalThroughput(), valid)
                + "</p><p><strong>Events consumed per service:</strong> Kafka Streams "
                + distribution(streams.eventsConsumedPerService()) + "; Plain Java "
                + distribution(plain.eventsConsumedPerService())
                + "</p><p class=\"muted\">" + range
                + "</p><table><thead><tr><th>Metric</th><th>Kafka Streams</th><th>Plain Java</th>"
                + "</tr></thead><tbody>" + rows + "</tbody></table></section>";
    }

    static String winnerSummary(double streams, double plain, boolean valid) {
        if (!valid) return "No winner because output validation failed.";
        if (rounded(streams) == rounded(plain)) return "Tie at the displayed precision.";
        double slower = Math.min(streams, plain);
        double difference = slower == 0 ? 0 : (Math.max(streams, plain) - slower) / slower * 100;
        String winner = streams > plain ? "Kafka Streams" : "Plain Java";
        return String.format(Locale.ROOT, "%s had %.1f%% higher total processing throughput.", winner, difference);
    }

    static String overallSummary(List<BenchmarkResult> results) {
        int streamsWins = 0, plainWins = 0, ties = 0, invalid = 0;
        for (Scenario scenario : scenarios(results)) {
            List<BenchmarkResult> streamsRuns = matching(results, scenario, "Kafka Streams");
            List<BenchmarkResult> plainRuns = matching(results, scenario, "Plain Java");
            if (streamsRuns.isEmpty() || plainRuns.isEmpty()) continue;
            boolean valid = streamsRuns.stream().allMatch(result -> result.validation().valid())
                    && plainRuns.stream().allMatch(result -> result.validation().valid());
            if (!valid) { invalid++; continue; }
            double streams = Statistics.medianBy(streamsRuns, BenchmarkResult::totalThroughput).totalThroughput();
            double plain = Statistics.medianBy(plainRuns, BenchmarkResult::totalThroughput).totalThroughput();
            if (rounded(streams) == rounded(plain)) ties++;
            else if (streams > plain) streamsWins++;
            else plainWins++;
        }
        int valid = streamsWins + plainWins + ties;
        if (valid == 0) return invalid == 0
                ? "No overall winner yet: there are no complete Kafka Streams versus Plain Java comparisons."
                : "No overall winner: all " + invalid + " complete comparison" + plural(invalid)
                        + " failed output validation.";
        String summary;
        if (streamsWins > plainWins) {
            summary = "Kafka Streams did better overall: " + streamsWins + " configuration win"
                    + plural(streamsWins) + " versus " + plainWins
                    + " for Plain Java, with " + ties + " tie" + plural(ties) + ".";
        } else if (plainWins > streamsWins) {
            summary = "Plain Java did better overall: " + plainWins + " configuration win"
                    + plural(plainWins) + " versus " + streamsWins
                    + " for Kafka Streams, with " + ties + " tie" + plural(ties) + ".";
        } else {
            summary = "Overall result was tied: Kafka Streams and Plain Java each won " + streamsWins
                    + " configuration" + plural(streamsWins) + ", with " + ties + " tie" + plural(ties) + ".";
        }
        return invalid == 0 ? summary : summary + " " + invalid + " invalid comparison"
                + plural(invalid) + " excluded.";
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

    private static String neutralRow(String label, double left, double right, String suffix) {
        return String.format(Locale.ROOT, "<tr><td>%s</td><td>%,.2f%s</td><td>%,.2f%s</td></tr>",
                escape(label), left, suffix, right, suffix);
    }

    private static long rounded(double value) { return Math.round(value * 100); }
    private static List<Scenario> scenarios(List<BenchmarkResult> results) {
        return results.stream().map(result -> new Scenario(result.eventCount(), result.durationSeconds(),
                        result.outputIntervalSeconds(),
                        result.requestedPartitions(), result.actualPartitions(), result.serviceInstances()))
                .distinct().sorted(Comparator.comparingInt(Scenario::partitions)
                        .thenComparingInt(Scenario::serviceInstances)
                        .thenComparingInt(Scenario::eventCount)).toList();
    }
    private static List<BenchmarkResult> matching(List<BenchmarkResult> all, Scenario scenario, String implementation) {
        return all.stream().filter(r -> r.eventCount() == scenario.eventCount()
                && r.durationSeconds() == scenario.durationSeconds()
                && r.outputIntervalSeconds() == scenario.outputIntervalSeconds()
                && r.requestedPartitions() == scenario.partitions()
                && r.serviceInstances() == scenario.serviceInstances()
                && r.implementation().equals(implementation)).toList();
    }
    private static double min(List<BenchmarkResult> values) { return values.stream().mapToDouble(BenchmarkResult::totalThroughput).min().orElse(0); }
    private static double max(List<BenchmarkResult> values) { return values.stream().mapToDouble(BenchmarkResult::totalThroughput).max().orElse(0); }
    private static String number(long number) { return String.format(Locale.ROOT, "%,d", number); }
    private static String services(int count) { return count + (count == 1 ? " service" : " services"); }
    private static String environmentSummary(List<BenchmarkResult> results) {
        if (results.isEmpty()) return "";
        Object brokers = results.getFirst().safeConfiguration().get("brokerCount");
        Object replication = results.getFirst().safeConfiguration().get("replicationFactor");
        if (!(brokers instanceof Number brokerCount) || !(replication instanceof Number replicationFactor)) return "";
        Object processingMode = results.getFirst().safeConfiguration().get("processingMode");
        int count = brokerCount.intValue();
        return "<p class=\"lead\"><strong>Kafka environment:</strong> " + count
                + (count == 1 ? " broker" : " brokers") + ", replication factor "
                + replicationFactor.intValue() + ". This report contains only this broker-count environment.</p>"
                + (processingMode == null ? "" : "<p class=\"lead\"><strong>Processing workload:</strong> "
                + workloadDescription(processingMode.toString()) + "</p>");
    }
    private static String workloadDescription(String mode) {
        return "metadata".equals(mode)
                ? "Seeded variable-rate input, duplicate-event removal, and one globally consolidated track-ID payload per configured interval."
                : "One input event is transformed into one output event.";
    }
    private static String plural(int count) { return count == 1 ? "" : "s"; }
    private static String distribution(List<Integer> counts) { return counts.stream().map(ReportWriter::number).collect(java.util.stream.Collectors.joining(" / ")); }
    private static boolean isMetadata(BenchmarkResult result) {
        return "metadata".equals(result.safeConfiguration().get("processingMode"));
    }
    private static String escape(String value) { return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;"); }

    private record Scenario(int eventCount, int durationSeconds, int outputIntervalSeconds,
                            int partitions, int actual, int serviceInstances) {}
    private record Summary(List<BenchmarkResult> results, List<SkippedScenario> skippedScenarios) {}
}
