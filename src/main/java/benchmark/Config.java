package benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

record Config(
        String bootstrapServers, Path truststore, String truststorePassword,
        String inputTopic, String outputTopic, List<Integer> eventCounts,
        List<Integer> partitions, int payloadBytes, int uniqueKeys, int threads,
        int warmupEvents, int iterations, long inputRate, int replicationFactor,
        int httpPort, Path resultsDir, int timeoutSeconds, String processingGuarantee,
        Map<String, String> extraKafkaProperties) {

    static Config from(Map<String, String> env) {
        String bootstrap = required(env, "KAFKA_BOOTSTRAP_SERVERS");
        Path truststore = Path.of(required(env, "KAFKA_TRUSTSTORE_LOCATION"));
        String password = required(env, "KAFKA_TRUSTSTORE_PASSWORD");
        var eventCounts = positiveList(env.getOrDefault("BENCHMARK_EVENT_COUNTS", "10000,100000,1000000"), "BENCHMARK_EVENT_COUNTS");
        var partitions = positiveList(env.getOrDefault("BENCHMARK_PARTITIONS", "1,3,6,12"), "BENCHMARK_PARTITIONS");
        for (int i = 1; i < partitions.size(); i++) {
            if (partitions.get(i) <= partitions.get(i - 1)) {
                throw new IllegalArgumentException("BENCHMARK_PARTITIONS must be strictly ascending");
            }
        }
        Map<String, String> extras = new LinkedHashMap<>();
        env.forEach((key, value) -> {
            if (key.startsWith("KAFKA_PROPERTY_")) {
                extras.put(key.substring(15).toLowerCase().replace('_', '.'), value);
            }
        });
        Config config = new Config(bootstrap, truststore, password,
                env.getOrDefault("BENCHMARK_INPUT_TOPIC", "benchmark-input"),
                env.getOrDefault("BENCHMARK_OUTPUT_TOPIC", "benchmark-output"),
                eventCounts, partitions,
                positiveInt(env, "BENCHMARK_PAYLOAD_BYTES", 1024),
                positiveInt(env, "BENCHMARK_UNIQUE_KEYS", 1000),
                positiveInt(env, "BENCHMARK_PROCESSING_THREADS", 1),
                nonNegativeInt(env, "BENCHMARK_WARMUP_EVENTS", 1000),
                positiveInt(env, "BENCHMARK_ITERATIONS", 3),
                nonNegativeLong(env, "BENCHMARK_INPUT_RATE", 0),
                positiveInt(env, "BENCHMARK_REPLICATION_FACTOR", 1),
                positiveInt(env, "BENCHMARK_HTTP_PORT", 8080),
                Path.of(env.getOrDefault("BENCHMARK_RESULTS_DIR", "results")),
                positiveInt(env, "BENCHMARK_TIMEOUT_SECONDS", 600),
                env.getOrDefault("KAFKA_STREAMS_PROCESSING_GUARANTEE", "at_least_once"), extras);
        if (!Files.isRegularFile(config.truststore())) {
            throw new IllegalArgumentException("KAFKA_TRUSTSTORE_LOCATION is not a readable file: " + config.truststore());
        }
        return config;
    }

    Properties kafkaProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("security.protocol", "SSL");
        properties.put("ssl.truststore.location", truststore.toString());
        properties.put("ssl.truststore.password", truststorePassword);
        extraKafkaProperties.forEach(properties::put);
        return properties;
    }

    Map<String, Object> safeConfiguration() {
        Map<String, Object> safe = new LinkedHashMap<>();
        safe.put("bootstrapServers", bootstrapServers);
        safe.put("securityProtocol", "SSL");
        safe.put("truststoreLocation", truststore.toString());
        safe.put("inputTopic", inputTopic);
        safe.put("outputTopic", outputTopic);
        safe.put("payloadBytes", payloadBytes);
        safe.put("uniqueKeys", uniqueKeys);
        safe.put("processingThreads", threads);
        safe.put("inputRate", inputRate);
        return safe;
    }

    private static String required(Map<String, String> env, String key) {
        String value = env.get(key);
        if (value == null || value.isBlank()) throw new IllegalArgumentException(key + " is required");
        return value;
    }

    private static List<Integer> positiveList(String value, String name) {
        try {
            List<Integer> result = Arrays.stream(value.split(",")).map(String::trim).map(Integer::parseInt).toList();
            if (result.isEmpty() || result.stream().anyMatch(number -> number <= 0)) throw new NumberFormatException();
            return result;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be a comma-separated list of positive integers", exception);
        }
    }

    private static int positiveInt(Map<String, String> env, String key, int fallback) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value <= 0) throw new IllegalArgumentException(key + " must be positive");
        return value;
    }

    private static int nonNegativeInt(Map<String, String> env, String key, int fallback) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < 0) throw new IllegalArgumentException(key + " must not be negative");
        return value;
    }

    private static long nonNegativeLong(Map<String, String> env, String key, long fallback) {
        long value = Long.parseLong(env.getOrDefault(key, Long.toString(fallback)));
        if (value < 0) throw new IllegalArgumentException(key + " must not be negative");
        return value;
    }
}
