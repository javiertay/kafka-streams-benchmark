package benchmark;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ConfigTest {
    @Test void parsesConfigurationAndKeepsSecretsOutOfSafeView(@TempDir Path directory) throws Exception {
        Path truststore = Files.createFile(directory.resolve("truststore.jks"));
        Map<String, String> env = new HashMap<>();
        env.put("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093");
        env.put("KAFKA_TRUSTSTORE_LOCATION", truststore.toString());
        env.put("KAFKA_TRUSTSTORE_PASSWORD", "secret");
        env.put("BENCHMARK_EVENT_COUNTS", "10, 20");
        env.put("BENCHMARK_PARTITIONS", "1,3");
        env.put("KAFKA_PROPERTY_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https");
        Config config = Config.from(env);
        assertEquals(java.util.List.of(10, 20), config.eventCounts());
        assertEquals("https", config.extraKafkaProperties().get("ssl.endpoint.identification.algorithm"));
        assertFalse(config.safeConfiguration().toString().contains("secret"));
    }

    @Test void rejectsDescendingPartitions(@TempDir Path directory) throws Exception {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "KAFKA_TRUSTSTORE_LOCATION", Files.createFile(directory.resolve("truststore.jks")).toString(),
                "KAFKA_TRUSTSTORE_PASSWORD", "secret", "BENCHMARK_PARTITIONS", "3,1");
        assertThrows(IllegalArgumentException.class, () -> Config.from(env));
    }
}
