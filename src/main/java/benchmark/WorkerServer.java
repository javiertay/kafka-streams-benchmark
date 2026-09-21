package benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

final class WorkerServer {
    private static final ObjectMapper JSON = new ObjectMapper().findAndRegisterModules();
    private final Config config;
    private ProcessorSession session;
    private WorkerCommand activeCommand;

    private WorkerServer(Config config) { this.config = config; }

    static void serve(Config config) throws IOException {
        WorkerServer worker = new WorkerServer(config);
        HttpServer server = HttpServer.create(new InetSocketAddress(config.httpPort()), 0);
        server.createContext("/start", worker::start);
        server.createContext("/resume", worker::resume);
        server.createContext("/progress", worker::progress);
        server.createContext("/stop", worker::stop);
        server.setExecutor(java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        System.out.println("[worker] READY for coordinator commands on port " + config.httpPort());
    }

    private synchronized void start(HttpExchange exchange) throws IOException {
        try {
            WorkerCommand command = JSON.readValue(exchange.getRequestBody(), WorkerCommand.class);
            if (session != null && activeCommand.runId().equals(command.runId())) {
                respond(exchange, 200, "ready");
                return;
            }
            if (session != null) throw new IllegalStateException("Worker is already running a scenario");
            System.out.printf("[worker] STARTING %s run %s%n", command.implementation(), shortRunId(command.runId()));
            session = ProcessorSession.start(config, command);
            activeCommand = command;
            System.out.printf("[worker] RUNNING %s run %s%n", command.implementation(), shortRunId(command.runId()));
            respond(exchange, 200, "ready");
        } catch (Exception exception) {
            System.err.println("[worker] START FAILED: " + exception.getMessage());
            respond(exchange, 500, exception.getMessage());
        }
    }

    private synchronized void stop(HttpExchange exchange) throws IOException {
        try {
            if (session == null) throw new IllegalStateException("Worker has no active scenario");
            WorkerCommand command = activeCommand;
            System.out.printf("[worker] STOPPING %s run %s%n", command.implementation(), shortRunId(command.runId()));
            ProcessorReport report = session.stop();
            session = null;
            activeCommand = null;
            System.out.printf("[worker] STOPPED %s run %s: %,d consumed, %,d published%n",
                    command.implementation(), shortRunId(command.runId()), report.consumed(), report.published());
            respond(exchange, 200, JSON.writeValueAsString(report));
        } catch (Exception exception) {
            System.err.println("[worker] STOP FAILED: " + exception.getMessage());
            respond(exchange, 500, exception.getMessage());
        }
    }

    private synchronized void resume(HttpExchange exchange) throws IOException {
        try {
            if (session == null) throw new IllegalStateException("Worker has no active scenario");
            session.resume();
            respond(exchange, 200, "running");
        } catch (Exception exception) {
            respond(exchange, 500, exception.getMessage());
        }
    }

    private synchronized void progress(HttpExchange exchange) throws IOException {
        try {
            if (session == null) throw new IllegalStateException("Worker has no active scenario");
            respond(exchange, 200, JSON.writeValueAsString(session.progress()));
        } catch (Exception exception) {
            respond(exchange, 500, exception.getMessage());
        }
    }

    private static String shortRunId(String runId) {
        return runId.substring(Math.max(0, runId.length() - 8));
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = (body == null ? "" : body).getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}
