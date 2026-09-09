package benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

final class DistributedWorkers implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    private final HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    private final List<String> urls;
    private final Set<String> startedUrls = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final Duration requestTimeout;
    private boolean stopped;

    private DistributedWorkers(List<String> urls, int timeoutSeconds) {
        this.urls = urls;
        requestTimeout = Duration.ofSeconds(timeoutSeconds + 5L);
    }

    static DistributedWorkers start(Config config, int count, WorkerCommand command) throws Exception {
        if (count > config.workerUrls().size())
            throw new IllegalArgumentException("Scenario needs " + count + " worker services but only "
                    + config.workerUrls().size() + " are configured");
        DistributedWorkers workers = new DistributedWorkers(
                config.workerUrls().stream().limit(count).toList(), config.timeoutSeconds());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var starts = workers.urls.stream()
                    .map(url -> executor.submit(() -> { workers.startPeer(url, command); return null; }))
                    .toList();
            for (var start : starts) start.get();
            return workers;
        } catch (ExecutionException exception) {
            workers.stopStartedPeers();
            if (exception.getCause() instanceof Exception cause) throw cause;
            throw exception;
        } catch (Exception exception) {
            workers.stopStartedPeers();
            throw exception;
        }
    }

    private void startPeer(String url, WorkerCommand command) throws Exception {
        long deadline = System.nanoTime() + requestTimeout.toNanos();
        Exception last = null;
        System.out.println("[workers] STARTING " + url);
        while (System.nanoTime() < deadline) {
            try {
                post(url + "/start", JSON.writeValueAsString(command));
                startedUrls.add(url);
                System.out.println("[workers] READY " + url);
                return;
            }
            catch (IOException exception) { last = exception; Thread.sleep(250); }
        }
        throw new IOException("Worker did not become ready within " + requestTimeout.toSeconds()
                + " seconds: " + url, last);
    }

    List<ProcessorReport> stop() throws Exception {
        if (stopped) return List.of();
        stopped = true;
        List<ProcessorReport> reports = new ArrayList<>();
        for (String url : urls) reports.add(JSON.readValue(post(url + "/stop", ""), ProcessorReport.class));
        return reports;
    }

    private void stopStartedPeers() {
        for (String url : urls) {
            if (!startedUrls.contains(url)) continue;
            try { post(url + "/stop", ""); }
            catch (Exception cleanupFailure) {
                System.err.println("[workers] Cleanup failed for " + url + ": " + cleanupFailure.getMessage());
            }
        }
    }

    private String post(String url, String body) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(requestTimeout)
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200)
            throw new IOException(url + " returned " + response.statusCode() + ": " + response.body());
        return response.body();
    }

    @Override public void close() throws Exception { stop(); }
}
