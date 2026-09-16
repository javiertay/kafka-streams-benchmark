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
        String bootstrapServers, int brokerCount, String securityProtocol, Path truststore, String truststorePassword,
        String inputTopic, String partialTopic, String outputTopic, List<Integer> partitions,
        int payloadBytes, int uniqueKeys, List<Integer> serviceInstances,
        int warmupDurationSeconds, int durationSeconds, int outputIntervalSeconds,
        int minInputsPerSecond, int maxInputsPerSecond, int duplicatePercent, long workloadSeed,
        int iterations, int replicationFactor,
        int httpPort, Path resultsDir, int timeoutSeconds, String processingGuarantee,
        ProcessingMode processingMode, List<String> workerUrls, Map<String, String> extraKafkaProperties,
        int framesPerSecond, int simulatedJobs, int minDetectionsPerFrame, int maxDetectionsPerFrame,
        int minVehicleCount, double maxVehicleVelocityKmh, int congestionMinDurationSeconds,
        int clearDurationSeconds, double metersPerPixel, List<Point> roiPolygon) {

    static Config from(Map<String, String> env) {
        String bootstrap = required(env, "KAFKA_BOOTSTRAP_SERVERS");
        int brokerCount = positiveInt(env, "KAFKA_BROKER_COUNT", 1);
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
        int durationSeconds = positiveInt(env, "BENCHMARK_DURATION_SECONDS", 300);
        int outputIntervalSeconds = positiveInt(env, "BENCHMARK_OUTPUT_INTERVAL_SECONDS", 5);
        int minInputsPerSecond = nonNegativeInt(env, "BENCHMARK_MIN_INPUTS_PER_SECOND", 10);
        int maxInputsPerSecond = positiveInt(env, "BENCHMARK_MAX_INPUTS_PER_SECOND", 500);
        if (minInputsPerSecond > maxInputsPerSecond) {
            throw new IllegalArgumentException("BENCHMARK_MIN_INPUTS_PER_SECOND must not exceed BENCHMARK_MAX_INPUTS_PER_SECOND");
        }
        int duplicatePercent = boundedPercent(env, "BENCHMARK_DUPLICATE_PERCENT", 20);
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
        int replicationFactor = positiveInt(env, "BENCHMARK_REPLICATION_FACTOR", brokerCount);
        if (replicationFactor > brokerCount) {
            throw new IllegalArgumentException("BENCHMARK_REPLICATION_FACTOR must not exceed KAFKA_BROKER_COUNT");
        }
        Config config = new Config(bootstrap, brokerCount, securityProtocol, truststore, password,
                env.getOrDefault("BENCHMARK_INPUT_TOPIC", "benchmark-input"),
                env.getOrDefault("BENCHMARK_PARTIAL_TOPIC", "benchmark-metadata-partials"),
                env.getOrDefault("BENCHMARK_OUTPUT_TOPIC", "benchmark-output"),
                partitions,
                positiveInt(env, "BENCHMARK_PAYLOAD_BYTES", 1024),
                positiveInt(env, "BENCHMARK_UNIQUE_KEYS", 1000),
                serviceInstances,
                nonNegativeInt(env, "BENCHMARK_WARMUP_DURATION_SECONDS", 10),
                durationSeconds, outputIntervalSeconds, minInputsPerSecond, maxInputsPerSecond,
                duplicatePercent, Long.parseLong(env.getOrDefault("BENCHMARK_WORKLOAD_SEED", "42")),
                fairIterations(env),
                replicationFactor,
                positiveInt(env, "BENCHMARK_HTTP_PORT", 8080),
                Path.of(env.getOrDefault("BENCHMARK_RESULTS_DIR", "results")),
                positiveInt(env, "BENCHMARK_TIMEOUT_SECONDS", 600),
                env.getOrDefault("KAFKA_STREAMS_PROCESSING_GUARANTEE", "at_least_once"),
                ProcessingMode.parse(env.getOrDefault("BENCHMARK_PROCESSING_MODE", "transform")),
                Arrays.stream(env.getOrDefault("BENCHMARK_WORKER_URLS", "").split(","))
                        .map(String::trim).filter(value -> !value.isEmpty()).toList(), extras,
                positiveInt(env, "BENCHMARK_FRAMES_PER_SECOND", 5),
                positiveInt(env, "BENCHMARK_SIMULATED_JOBS", 12),
                nonNegativeInt(env, "BENCHMARK_MIN_DETECTIONS_PER_FRAME", 5),
                positiveInt(env, "BENCHMARK_MAX_DETECTIONS_PER_FRAME", 25),
                positiveInt(env, "BENCHMARK_MIN_VEHICLE_COUNT", 10),
                positiveDouble(env, "BENCHMARK_MAX_VEHICLE_VELOCITY_KMH", 5),
                congestionDuration(env),
                positiveInt(env, "BENCHMARK_CLEAR_DURATION_SECONDS", 10),
                positiveDouble(env, "BENCHMARK_METERS_PER_PIXEL", 0.05),
                roi(env.getOrDefault("BENCHMARK_ROI_POLYGON", "0,0;1920,0;1920,1080;0,1080")));
        if (config.minDetectionsPerFrame() > config.maxDetectionsPerFrame())
            throw new IllegalArgumentException("BENCHMARK_MIN_DETECTIONS_PER_FRAME must not exceed BENCHMARK_MAX_DETECTIONS_PER_FRAME");
        if (config.processingMode() == ProcessingMode.VEHICLE_CONGESTION
                && config.maxDetectionsPerFrame() < config.minVehicleCount())
            throw new IllegalArgumentException("BENCHMARK_MAX_DETECTIONS_PER_FRAME must be at least BENCHMARK_MIN_VEHICLE_COUNT");
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
        safe.put("brokerCount", brokerCount);
        safe.put("securityProtocol", securityProtocol);
        if (truststore != null) safe.put("truststoreLocation", truststore.toString());
        safe.put("inputTopic", inputTopic);
        safe.put("partialTopic", partialTopic);
        safe.put("outputTopic", outputTopic);
        safe.put("payloadBytes", payloadBytes);
        safe.put("uniqueKeys", uniqueKeys);
        safe.put("serviceInstanceScenarios", serviceInstances);
        safe.put("durationSeconds", durationSeconds);
        safe.put("outputIntervalSeconds", outputIntervalSeconds);
        safe.put("minInputsPerSecond", minInputsPerSecond);
        safe.put("maxInputsPerSecond", maxInputsPerSecond);
        safe.put("duplicatePercent", duplicatePercent);
        safe.put("workloadSeed", workloadSeed);
        safe.put("warmupDurationSeconds", warmupDurationSeconds);
        safe.put("replicationFactor", replicationFactor);
        safe.put("processingMode", processingMode.name().toLowerCase(Locale.ROOT));
        if (processingMode == ProcessingMode.VEHICLE_CONGESTION) {
            safe.put("framesPerSecond", framesPerSecond);
            safe.put("simulatedJobs", simulatedJobs);
            safe.put("minDetectionsPerFrame", minDetectionsPerFrame);
            safe.put("maxDetectionsPerFrame", maxDetectionsPerFrame);
            safe.put("minVehicleCount", minVehicleCount);
            safe.put("maxVehicleVelocityKmh", maxVehicleVelocityKmh);
            safe.put("minDurationSeconds", congestionMinDurationSeconds);
            safe.put("clearDurationSeconds", clearDurationSeconds);
            safe.put("metersPerPixel", metersPerPixel);
            safe.put("roiPolygon", roiPolygon);
        }
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

    private static int fairIterations(Map<String, String> env) {
        int value = positiveInt(env, "BENCHMARK_ITERATIONS", 4);
        if (value < 2 || value % 2 != 0) {
            throw new IllegalArgumentException("BENCHMARK_ITERATIONS must be an even number of at least 2 so run order is balanced");
        }
        return value;
    }

    private static int nonNegativeInt(Map<String, String> env, String key, int fallback) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < 0) throw new IllegalArgumentException(key + " must not be negative");
        return value;
    }

    private static int boundedPercent(Map<String, String> env, String key, int fallback) {
        int value = Integer.parseInt(env.getOrDefault(key, Integer.toString(fallback)));
        if (value < 0 || value > 100) throw new IllegalArgumentException(key + " must be between 0 and 100");
        return value;
    }

    private static double positiveDouble(Map<String, String> env, String key, double fallback) {
        double value = Double.parseDouble(env.getOrDefault(key, Double.toString(fallback)));
        if (!Double.isFinite(value) || value <= 0) throw new IllegalArgumentException(key + " must be positive");
        return value;
    }

    private static int congestionDuration(Map<String, String> env) {
        String preset = env.getOrDefault("BENCHMARK_CONGESTION_PRESET", "normal_road_segment");
        int fallback = switch (preset) {
            case "signalized_junction" -> 120;
            case "normal_road_segment" -> 0;
            default -> throw new IllegalArgumentException("BENCHMARK_CONGESTION_PRESET must be signalized_junction or normal_road_segment");
        };
        return nonNegativeInt(env, "BENCHMARK_CONGESTION_MIN_DURATION_SECONDS", fallback);
    }

    private static List<Point> roi(String value) {
        try {
            List<Point> points = Arrays.stream(value.split(";")).map(pair -> {
                String[] coordinates = pair.trim().split(",");
                if (coordinates.length != 2) throw new IllegalArgumentException();
                return new Point(Double.parseDouble(coordinates[0].trim()), Double.parseDouble(coordinates[1].trim()));
            }).toList();
            if (points.size() < 3) throw new IllegalArgumentException();
            return points;
        } catch (RuntimeException exception) {
            throw new IllegalArgumentException("BENCHMARK_ROI_POLYGON must contain at least three x,y points separated by semicolons", exception);
        }
    }

}

enum ProcessingMode {
    TRANSFORM, METADATA, VEHICLE_CONGESTION;

    static ProcessingMode parse(String value) {
        try {
            return valueOf(value.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("BENCHMARK_PROCESSING_MODE must be transform, metadata, or vehicle_congestion", exception);
        }
    }
}
