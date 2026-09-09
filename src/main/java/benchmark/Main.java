package benchmark;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws Exception {
        Config config;
        try {
            config = Config.from(System.getenv());
        } catch (IllegalArgumentException exception) {
            System.err.println("Configuration error: " + exception.getMessage());
            System.exit(2);
            return;
        }
        Files.createDirectories(config.resultsDir());
        if (Boolean.parseBoolean(System.getenv().getOrDefault("BENCHMARK_WORKER_MODE", "false"))) {
            WorkerServer.serve(config);
            return;
        }
        boolean succeeded = false;
        try {
            System.out.println("[benchmark] Coordinator starting");
            new BenchmarkOrchestrator().run(config);
            succeeded = true;
            System.out.println("[benchmark] All benchmark work finished successfully");
        } catch (Exception exception) {
            System.err.println("[benchmark] FAILED: " + (exception.getMessage() == null
                    ? exception.getClass().getSimpleName() : exception.getMessage()));
            exception.printStackTrace(System.err);
            writeFailure(config.resultsDir(), exception);
        }
        serve(config.resultsDir(), config.httpPort(), succeeded);
    }

    private static void serve(Path directory, int port, boolean benchmarkSucceeded) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/", exchange -> respond(exchange, directory));
        server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.printf("[server] READY: benchmark %s; results available at http://localhost:%d%n",
                benchmarkSucceeded ? "complete" : "failed", port);
    }

    private static void respond(HttpExchange exchange, Path directory) throws IOException {
        String requestPath = exchange.getRequestURI().getPath();
        String filename = requestPath.equals("/") ? "index.html" : requestPath.substring(1);
        Path file = directory.resolve(filename).normalize();
        if (!file.startsWith(directory.normalize()) || !Files.isRegularFile(file)) {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
            return;
        }
        String contentType = filename.endsWith(".json") ? "application/json; charset=utf-8" : "text/html; charset=utf-8";
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(200, Files.size(file));
        try (var output = exchange.getResponseBody()) { Files.copy(file, output); }
    }

    private static void writeFailure(Path directory, Exception exception) throws IOException {
        String message = exception.getMessage() == null ? exception.getClass().getSimpleName() : exception.getMessage();
        String safe = message.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        Files.writeString(directory.resolve("index.html"), "<!doctype html><html><head><meta charset=\"utf-8\"><title>Benchmark failed</title></head><body><h1>Benchmark failed</h1><p>" + safe + "</p><p>Review the container logs and Kafka SSL configuration.</p></body></html>");
    }
}
