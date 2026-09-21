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
                .metric-help{cursor:help;text-decoration:underline dotted;text-underline-offset:.2em}.metric-help:focus{outline:2px solid #3157c8;outline-offset:2px;border-radius:3px}
                details{margin-top:1rem}code{background:#edf1f7;padding:.1rem .3rem;border-radius:4px}@media(max-width:700px){main{padding:.7rem}.card{overflow-x:auto}th,td{white-space:nowrap}}
                .config-tabs{display:flex;gap:.5rem;overflow-x:auto;padding:.25rem 0 1rem}.config-tab{white-space:nowrap;border:1px solid #aeb9ca;border-radius:999px;background:#fff;padding:.6rem .9rem;cursor:pointer}.config-tab[aria-selected=true]{background:#3157c8;color:#fff;border-color:#3157c8}.scenario-panel[hidden]{display:none}
                </style></head><body><main>
                <header><h1>Kafka Streams vs Plain Java</h1><p class="lead purpose"><strong>What performance are we testing?</strong> This benchmark streams the same deterministic workload through both implementations and compares equivalent JSON decoding, business-rule processing, and output publication.</p>
                <p class="lead"><strong>Why are we doing this?</strong> To make an evidence-based choice between Kafka Streams and direct <code>KafkaConsumer</code>/<code>KafkaProducer</code> code under the same Java 25 runtime, Kafka cluster, workload, partitions, processing logic, and resource limits. Results describe this environment only; they are not universal performance claims.</p>
                """ + environment + """
                <p class="muted">Green marks the better displayed value. Values that round to the same display precision are ties. Invalid runs never receive a winner.</p></header>
                <section class="card"><h2>Overall summary</h2><p class="verdict"><strong>""" + escape(overallSummary(results)) + """
                </strong></p><p class="muted">Each complete, valid configuration gets one vote based on median total processing throughput. Workloads are not averaged together.</p></section>
                <section class="card"><h2>How to read the results</h2><p><strong>Scheduled input records:</strong> the deterministic total replayed by each implementation; in vehicle-congestion mode each record is one complete frame whose detection count varies. <strong>Expected outputs:</strong> one per input in transform mode, one consolidated payload per interval in metadata mode, or one finding per congestion episode in vehicle-congestion mode. <strong>Total elapsed time:</strong> worker release until all scheduled inputs are consumed and all expected outputs are observed. <strong>CPU/RAM:</strong> totals across processor services only; preparation, generator, and observer resources are excluded.</p></section>
                """ + ingestionSection(results) + processingSection(results)
                + (tabs.isEmpty() ? "" : "<section class=\"card\"><h2>End-to-end benchmark</h2><p class=\"muted\">The original complete-pipeline comparison remains below: consume, decode, apply the business rule, publish, and validate every expected output.</p></section>")
                + (tabs.isEmpty() ? "" : "<nav class=\"config-tabs\" role=\"tablist\" aria-label=\"Benchmark configurations\">" + tabs + "</nav>")
                + scenarios + (skipHtml.isEmpty() ? "" : "<section class=\"card\"><h2>Skipped scenarios</h2><ul>" + skipHtml + "</ul></section>") + """
                <section class="card"><h2>Advanced JVM metrics</h2><p>Runtime, GC, heap, and safe Kafka configuration are available in <a href="summary.json">summary.json</a> and the per-run raw JSON files. Credentials are never written.</p>
                <details><summary>Measurement notes and limitations</summary><p>Both implementations replay the same pre-produced records from captured Kafka offsets; dataset preparation is not measured. Processing is timed identically from JSON decoding through business logic and output handoff. Kafka Streams does not expose a per-record producer acknowledgement callback, so publishing latency is measured from business-logic completion until the output observer receives the record. Latency percentiles use at most 100,000 evenly spaced samples per stage. Historical records are filtered by workload and execution IDs.</p></details></section>
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
        if (!isVehicleCongestion(streams))
            rows.append(neutralRow("Output interval", streams.outputIntervalSeconds(), plain.outputIntervalSeconds(), " s"));
        rows.append(neutralRow("Expected outputs", streams.validation().expected(), plain.validation().expected(), ""));
        rows.append(metricRow("Ingestion elapsed time", streams.ingestionElapsedSeconds(), plain.ingestionElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Ingestion throughput", streams.ingestionThroughput(), plain.ingestionThroughput(), true, "/s", valid));
        rows.append(metricRow("Processing elapsed time", streams.processingElapsedSeconds(), plain.processingElapsedSeconds(), false, " s", valid));
        rows.append(metricRow("Processing throughput", streams.processingThroughput(), plain.processingThroughput(), true, "/s", valid));
        rows.append(metricRow("Processing p50", streams.processingLatency().p50Ms(), plain.processingLatency().p50Ms(), false, " ms", valid));
        rows.append(metricRow("Processing p95", streams.processingLatency().p95Ms(), plain.processingLatency().p95Ms(), false, " ms", valid));
        rows.append(metricRow("Processing p99", streams.processingLatency().p99Ms(), plain.processingLatency().p99Ms(), false, " ms", valid));
        rows.append(metricRow("Processing max", streams.processingLatency().maxMs(), plain.processingLatency().maxMs(), false, " ms", valid));
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
                + "</p><p class=\"muted\">Hover over or focus the information marker beside a metric to see how it is measured.</p>"
                + "<table><thead><tr><th>Metric</th><th>Kafka Streams</th><th>Plain Java</th>"
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
                metricLabel(label), leftClass, left, suffix, rightClass, right, suffix);
    }

    private String ingestionSection(List<BenchmarkResult> all) {
        StringBuilder body = new StringBuilder();
        for (Scenario scenario : scenarios(all)) {
            List<BenchmarkResult> streamsRuns = matching(all, scenario, "Kafka Streams", BenchmarkType.INGESTION_ONLY);
            List<BenchmarkResult> plainRuns = matching(all, scenario, "Plain Java", BenchmarkType.INGESTION_ONLY);
            if (streamsRuns.isEmpty() || plainRuns.isEmpty()) continue;
            BenchmarkResult streams = Statistics.medianBy(streamsRuns, BenchmarkResult::totalThroughput);
            BenchmarkResult plain = Statistics.medianBy(plainRuns, BenchmarkResult::totalThroughput);
            boolean valid = streamsRuns.stream().allMatch(r -> r.validation().valid())
                    && plainRuns.stream().allMatch(r -> r.validation().valid());
            body.append("<h3>").append(number(scenario.eventCount())).append(" events Â· ")
                    .append(scenario.actual()).append(" partitions Â· ").append(services(scenario.serviceInstances()))
                    .append("</h3><p><strong>Consumed per service:</strong> Kafka Streams ")
                    .append(distribution(streams.eventsConsumedPerService())).append("; Plain Java ")
                    .append(distribution(plain.eventsConsumedPerService())).append("</p><table><thead><tr><th>Metric</th><th>Kafka Streams</th><th>Plain Java</th></tr></thead><tbody>")
                    .append(metricRow("Pure ingestion elapsed time", streams.totalElapsedSeconds(), plain.totalElapsedSeconds(), false, " s", valid))
                    .append(metricRow("Pure ingestion throughput", streams.totalThroughput(), plain.totalThroughput(), true, "/s", valid))
                    .append(metricRow("Pure ingestion bandwidth", megabytesPerSecond(streams), megabytesPerSecond(plain), true, " MB/s", valid))
                    .append(metricRow("Average CPU", streams.resources().averageCpuPercent(), plain.resources().averageCpuPercent(), false, "%", valid))
                    .append(metricRow("Peak RAM", streams.resources().peakRamMb(), plain.resources().peakRamMb(), false, " MB", valid))
                    .append("</tbody></table>");
        }
        if (body.isEmpty()) return "";
        return "<section class=\"card\"><h2>Pure ingestion</h2><p class=\"muted\">Consumes the pre-produced Kafka backlog and counts matching records only. It performs no JSON decoding, business rule, or output publication, so this is the framework ingestion ceiling.</p>" + body + "</section>";
    }

    private String processingSection(List<BenchmarkResult> all) {
        StringBuilder body = new StringBuilder();
        for (Scenario scenario : scenarios(all)) {
            List<BenchmarkResult> streamsRuns = matching(all, scenario, "Kafka Streams", BenchmarkType.END_TO_END);
            List<BenchmarkResult> plainRuns = matching(all, scenario, "Plain Java", BenchmarkType.END_TO_END);
            if (streamsRuns.isEmpty() || plainRuns.isEmpty()) continue;
            BenchmarkResult streams = Statistics.medianBy(streamsRuns, BenchmarkResult::activeProcessingThroughput);
            BenchmarkResult plain = Statistics.medianBy(plainRuns, BenchmarkResult::activeProcessingThroughput);
            boolean valid = streamsRuns.stream().allMatch(r -> r.validation().valid())
                    && plainRuns.stream().allMatch(r -> r.validation().valid());
            body.append("<h3>").append(number(scenario.eventCount())).append(" events Â· ")
                    .append(scenario.actual()).append(" partitions Â· ").append(services(scenario.serviceInstances()))
                    .append("</h3><table><thead><tr><th>Metric</th><th>Kafka Streams</th><th>Plain Java</th></tr></thead><tbody>")
                    .append(metricRow("Active processing time", streams.activeProcessingSeconds(), plain.activeProcessingSeconds(), false, " s", valid))
                    .append(metricRow("Active processing throughput", streams.activeProcessingThroughput(), plain.activeProcessingThroughput(), true, "/s", valid))
                    .append(metricRow("Processing mean", streams.meanProcessingLatencyMs(), plain.meanProcessingLatencyMs(), false, " ms", valid))
                    .append(metricRow("Processing p50", streams.processingLatency().p50Ms(), plain.processingLatency().p50Ms(), false, " ms", valid))
                    .append(metricRow("Processing p95", streams.processingLatency().p95Ms(), plain.processingLatency().p95Ms(), false, " ms", valid))
                    .append(metricRow("Processing p99", streams.processingLatency().p99Ms(), plain.processingLatency().p99Ms(), false, " ms", valid))
                    .append(metricRow("Processing max", streams.processingLatency().maxMs(), plain.processingLatency().maxMs(), false, " ms", valid))
                    .append("</tbody></table>");
        }
        if (body.isEmpty()) return "";
        return "<section class=\"card\"><h2>Pure processing</h2><p class=\"muted\">Uses the existing per-record timer around JSON decoding, the real business rule, and output creation/handoff. Kafka poll gaps and producer pacing are excluded; with multiple services, capacity uses the busiest worker's accumulated processing time.</p>" + body + "</section>";
    }

    private static String neutralRow(String label, double left, double right, String suffix) {
        return String.format(Locale.ROOT, "<tr><td>%s</td><td>%,.2f%s</td><td>%,.2f%s</td></tr>",
                metricLabel(label), left, suffix, right, suffix);
    }

    private static String metricLabel(String label) {
        String explanation = switch (label) {
            case "Scheduled input records" -> "Number of data records produced by the deterministic workload; in vehicle-congestion mode one record is one complete camera frame. Internal interval markers are excluded.";
            case "Run duration" -> "Configured time during which the generator streams the selected workload; warm-up is separate.";
            case "Output interval" -> "Configured number of seconds consolidated into each metadata output payload.";
            case "Expected outputs" -> "Number of validated outputs required for completion: transformed records, metadata interval payloads, or vehicle-congestion findings, depending on the selected mode.";
            case "Ingestion elapsed time" -> "Wall-clock span from the first matching input accepted by any worker to the last matching input accepted.";
            case "Ingestion throughput" -> "Consumed input records divided by ingestion elapsed time.";
            case "Processing elapsed time" -> "Wall-clock span from the first input processing completion to the last, aggregated across worker services.";
            case "Processing throughput" -> "Consumed input records divided by processing elapsed time.";
            case "Processing p50" -> "Median per-input time from before JSON decoding through the selected business logic and output handoff.";
            case "Processing p95" -> "95th-percentile per-input time from before JSON decoding through the selected business logic and output handoff.";
            case "Processing p99" -> "99th-percentile per-input time from before JSON decoding through the selected business logic and output handoff.";
            case "Processing max" -> "Slowest sampled per-input time from before JSON decoding through the selected business logic and output handoff.";
            case "Processing mean" -> "Exact accumulated processing time divided by the exact number of processed inputs; sampling is not used.";
            case "Active processing time" -> "Largest accumulated per-record processing time among the worker services, representing parallel busy time without Kafka poll gaps.";
            case "Active processing throughput" -> "All consumed inputs divided by the busiest worker's accumulated processing time; measures processing capacity while excluding input pacing and Kafka poll gaps.";
            case "Pure ingestion elapsed time" -> "Time from simultaneously releasing the stable consumers until every pre-produced input record is consumed; startup and dataset production are excluded.";
            case "Pure ingestion throughput" -> "All pre-produced input records divided by pure-ingestion elapsed time; JSON decoding, business logic, and output publication are disabled.";
            case "Pure ingestion bandwidth" -> "Exact serialized Kafka key and value bytes consumed, divided by pure-ingestion elapsed time. Kafka protocol overhead is excluded.";
            case "Publishing elapsed time" -> "Wall-clock span from the first expected output observed on Kafka to the last expected output observed.";
            case "Publishing throughput" -> "Observed output records divided by publishing elapsed time.";
            case "Publishing p99" -> "99th-percentile time from final output payload creation until the observer consumes it from Kafka.";
            case "Total elapsed time" -> "Time from releasing the stable workers onto the pre-produced backlog until every scheduled input is consumed and every expected output is observed; preparation, startup, and shutdown are excluded.";
            case "Total processing throughput" -> "Consumed input records divided by total elapsed time, including input pacing, consolidation, publication, and final output drain.";
            case "Average CPU" -> "Time-aligned average CPU usage summed across processor service JVMs; generator and observer are excluded.";
            case "Peak CPU" -> "Highest time-aligned total CPU usage across processor service JVMs; generator and observer are excluded.";
            case "Average RAM" -> "Time-aligned average resident memory summed across processor service JVMs; generator and observer are excluded.";
            case "Peak RAM" -> "Highest time-aligned total resident memory across processor service JVMs; generator and observer are excluded.";
            default -> null;
        };
        if (explanation == null) return escape(label);
        return "<span class=\"metric-help\" tabindex=\"0\" title=\"" + escape(explanation)
                + "\" aria-label=\"" + escape(label + ": " + explanation) + "\">"
                + escape(label) + " <span aria-hidden=\"true\">&#9432;</span></span>";
    }

    private static long rounded(double value) { return Math.round(value * 100); }
    private static List<Scenario> scenarios(List<BenchmarkResult> results) {
        return results.stream().filter(result -> result.benchmarkType() == BenchmarkType.END_TO_END)
                .map(result -> new Scenario(result.eventCount(), result.durationSeconds(),
                        result.outputIntervalSeconds(),
                        result.requestedPartitions(), result.actualPartitions(), result.serviceInstances()))
                .distinct().sorted(Comparator.comparingInt(Scenario::partitions)
                        .thenComparingInt(Scenario::serviceInstances)
                        .thenComparingInt(Scenario::eventCount)).toList();
    }
    private static List<BenchmarkResult> matching(List<BenchmarkResult> all, Scenario scenario, String implementation) {
        return matching(all, scenario, implementation, BenchmarkType.END_TO_END);
    }
    private static List<BenchmarkResult> matching(List<BenchmarkResult> all, Scenario scenario, String implementation,
                                                   BenchmarkType benchmarkType) {
        return all.stream().filter(r -> r.eventCount() == scenario.eventCount()
                && r.durationSeconds() == scenario.durationSeconds()
                && r.outputIntervalSeconds() == scenario.outputIntervalSeconds()
                && r.requestedPartitions() == scenario.partitions()
                && r.serviceInstances() == scenario.serviceInstances()
                && r.benchmarkType() == benchmarkType
                && r.implementation().equals(implementation)).toList();
    }
    private static double min(List<BenchmarkResult> values) { return values.stream().mapToDouble(BenchmarkResult::totalThroughput).min().orElse(0); }
    private static double max(List<BenchmarkResult> values) { return values.stream().mapToDouble(BenchmarkResult::totalThroughput).max().orElse(0); }
    private static double megabytesPerSecond(BenchmarkResult result) {
        return result.totalElapsedSeconds() == 0 ? 0
                : result.inputBytes() / 1_048_576.0 / result.totalElapsedSeconds();
    }
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
        return switch (mode) {
            case "metadata" -> "Seeded variable-rate input, duplicate-event removal, and one globally consolidated track-ID payload per configured interval.";
            case "vehicle_congestion" -> "Deterministic camera frames with varying detection counts, one-second velocity estimation, ROI filtering, congestion timers, and one finding per episode.";
            default -> "One input event is transformed into one output event.";
        };
    }
    private static boolean isVehicleCongestion(BenchmarkResult result) {
        return "vehicle_congestion".equals(result.safeConfiguration().get("processingMode"));
    }
    private static String plural(int count) { return count == 1 ? "" : "s"; }
    private static String distribution(List<Integer> counts) { return counts.stream().map(ReportWriter::number).collect(java.util.stream.Collectors.joining(" / ")); }
    private static String escape(String value) { return value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;"); }

    private record Scenario(int eventCount, int durationSeconds, int outputIntervalSeconds,
                            int partitions, int actual, int serviceInstances) {}
    private record Summary(List<BenchmarkResult> results, List<SkippedScenario> skippedScenarios) {}
}
