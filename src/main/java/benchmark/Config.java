package benchmark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

record Config(
        String bootstrapServers, String securityProtocol, Path truststore, String truststorePassword,
        String inputTopic, String outputTopic, List<Integer> partitions,
        int payloadBytes, int uniqueKeys, List<Integer> serviceInstances,
        int warmupSeconds, int measurementSeconds, int iterations, List<Long> inputRates, int replicationFactor,
        int httpPort, Path resultsDir, int timeoutSeconds, String processingGuarantee,
        List<String> workerUrls, Map<String, String> extraKafkaProperties) {

    static Config from(Map<String, String> env) {
        String bootstrap = required(env, "KAFKA_BOOTSTRAP_SERVERS");
        String securityProtocol = env.getOrDefault("KAFKA_SECURITY_PROTOCOL", "SSL").trim().toUpperCase(Locale.ROOT);
        if (!Set.of("PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL").contains(securityProtocol)) {
            throw new IllegalArgumentException(
                    "KAFKA_SECURITY_PROTOCOL must be PLAINTEXT, SSL, SASL_PLAINTEXT, or SASL_SSL");
        }
        boolean usesSsl = securityProtocol.endsWith("SSL");
        Path truststore = usesSsl ? Path.of(required(env, "KAFKA_TRUSTSTORE_LOCATION")) : null;
        String password = usesSsl ? required(env, "KAFKA_TRUSTSTORE_PASSWORD") : null;
        var partitions = positiveList(env.getOrDefault("BENCHMARK_PARTITIONS", "1,3,6,12"), "BENCHMARK_PARTITIONS");
        var serviceInstances = positiveList(env.getOrDefault("BENCHMARK_SERVICE_INSTANCES", "1"),
                "BENCHMARK_SERVICE_INSTANCES");
        var inputRates = positiveLongList(env.getOrDefault("BENCHMARK_INPUT_RATES", "100000,1000000"),
                "BENCHMARK_INPUT_RATES");
        int measurementSeconds = positiveInt(env, "BENCHMARK_MEASUREMENT_SECONDS", 30);
        int warmupSeconds = nonNegativeInt(env, "BENCHMARK_WARMUP_SECONDS", 2);
        for (long rate : inputRates) {
            long maximumEvents;
            try { maximumEvents = Math.multiplyExact(rate, Math.max(measurementSeconds, warmupSeconds)); }
            catch (ArithmeticException overflow) {
                throw new IllegalArgumentException("input rate × duration is too large", overflow);
            }
            if (maximumEvents > Integer.MAX_VALUE)
                throw new IllegalArgumentException("input rate × duration must not exceed 2,147,483,647 events");
        }
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
        Config config = new Config(bootstrap, securityProtocol, truststore, password,
                env.getOrDefault("BENCHMARK_INPUT_TOPIC", "benchmark-input"),
                env.getOrDefault("BENCHMARK_OUTPUT_TOPIC", "benchmark-output"),
                partitions,
                positiveInt(env, "BENCHMARK_PAYLOAD_BYTES", 1024),
                positiveInt(env, "BENCHMARK_UNIQUE_KEYS", 1000),
                serviceInstances,
                warmupSeconds,
                measurementSeconds,
                positiveInt(env, "BENCHMARK_ITERATIONS", 3),
                inputRates,
                positiveInt(env, "BENCHMARK_REPLICATION_FACTOR", 1),
                positiveInt(env, "BENCHMARK_HTTP_PORT", 8080),
                Path.of(env.getOrDefault("BENCHMARK_RESULTS_DIR", "results")),
                positiveInt(env, "BENCHMARK_TIMEOUT_SECONDS", 600),
                env.getOrDefault("KAFKA_STREAMS_PROCESSING_GUARANTEE", "at_least_once"),
                Arrays.stream(env.getOrDefault("BENCHMARK_WORKER_URLS", "").split(","))
                        .map(String::trim).filter(value -> !value.isEmpty()).toList(), extras);
        if (usesSsl && !Files.isRegularFile(config.truststore())) {
            throw new IllegalArgumentException("KAFKA_TRUSTSTORE_LOCATION is not a readable file: " + config.truststore());
        }
        return config;
    }

    Properties kafkaProperties() {
        Properties properties = new Properties();
        extraKafkaProperties.forEach(properties::put);
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("security.protocol", securityProtocol);
        if (truststore != null) {
            properties.put("ssl.truststore.location", truststore.toString());
            properties.put("ssl.truststore.password", truststorePassword);
        }
        return properties;
    }

    Map<String, Object> safeConfiguration() {
        Map<String, Object> safe = new LinkedHashMap<>();
        safe.put("bootstrapServers", bootstrapServers);
        safe.put("securityProtocol", securityProtocol);
        if (truststore != null) safe.put("truststoreLocation", truststore.toString());
        safe.put("inputTopic", inputTopic);
        safe.put("outputTopic", outputTopic);
        safe.put("payloadBytes", payloadBytes);
        safe.put("uniqueKeys", uniqueKeys);
        safe.put("serviceInstanceScenarios", serviceInstances);
        safe.put("inputRateScenarios", inputRates);
        safe.put("measurementSeconds", measurementSeconds);
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

    private static List<Long> positiveLongList(String value, String name) {
        try {
            List<Long> result = Arrays.stream(value.split(",")).map(String::trim).map(Long::parseLong).toList();
            if (result.isEmpty() || result.stream().anyMatch(number -> number <= 0)) throw new NumberFormatException();
            return result;
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(name + " must be a comma-separated list of positive integers", exception);
        }
    }
}
