package benchmark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.StreamsConfig;
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
        env.put("KAFKA_BROKER_COUNT", "3");
        env.put("BENCHMARK_PARTITIONS", "1,3");
        env.put("BENCHMARK_SERVICE_INSTANCES", "1,3,6");
        env.put("BENCHMARK_EVENT_COUNT", "500000");
        env.put("BENCHMARK_WARMUP_EVENT_COUNT", "50000");
        env.put("BENCHMARK_WORKER_URLS", "http://worker-1:8080, http://worker-2:8080");
        env.put("KAFKA_PROPERTY_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM", "https");
        Config config = Config.from(env);
        assertEquals(java.util.List.of(1, 3, 6), config.serviceInstances());
        assertEquals(500_000, config.eventCount());
        assertEquals(50_000, config.warmupEventCount());
        assertEquals(java.util.List.of("http://worker-1:8080", "http://worker-2:8080"), config.workerUrls());
        assertEquals(3, config.brokerCount());
        assertEquals(3, config.replicationFactor());
        assertEquals("SSL", config.securityProtocol());
        assertEquals("SSL", config.kafkaProperties().get("security.protocol"));
        assertEquals("https", config.extraKafkaProperties().get("ssl.endpoint.identification.algorithm"));
        assertFalse(config.safeConfiguration().toString().contains("secret"));
    }

    @Test void rejectsDescendingPartitions(@TempDir Path directory) throws Exception {
        Map<String, String> env = Map.of("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "KAFKA_TRUSTSTORE_LOCATION", Files.createFile(directory.resolve("truststore.jks")).toString(),
                "KAFKA_TRUSTSTORE_PASSWORD", "secret", "BENCHMARK_PARTITIONS", "3,1");
        assertThrows(IllegalArgumentException.class, () -> Config.from(env));
    }

    @Test void exposesReadableEquivalentClientProperties(@TempDir Path directory) throws Exception {
        Path truststore = Files.createFile(directory.resolve("truststore.jks"));
        Config config = Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "KAFKA_TRUSTSTORE_LOCATION", truststore.toString(),
                "KAFKA_TRUSTSTORE_PASSWORD", "secret",
                "BENCHMARK_SERVICE_INSTANCES", "3"));

        var producer = KafkaSupport.producerProperties(config);
        assertEquals("all", producer.get(ProducerConfig.ACKS_CONFIG));
        assertEquals(true, producer.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));

        var consumer = KafkaSupport.consumerProperties(config, "plain-run");
        assertEquals("plain-run", consumer.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals(false, consumer.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
        assertEquals(1000, consumer.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));

        var streams = KafkaSupport.streamsProperties(config, "streams-run", "run-id", 1);
        assertEquals("streams-run", streams.get(StreamsConfig.APPLICATION_ID_CONFIG));
        assertEquals(1, streams.get(StreamsConfig.NUM_STREAM_THREADS_CONFIG));
        assertEquals("at_least_once", streams.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG));
        assertEquals(KafkaSupport.COMMIT_INTERVAL_MS, streams.get(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG));
        assertEquals(1000, streams.get(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)));
        assertEquals("all", streams.get(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG)));
        assertEquals(true, streams.get(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)));
    }

    @Test void supportsPlaintextWithoutTruststore() {
        Config config = Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "plaintext"));

        assertEquals("PLAINTEXT", config.securityProtocol());
        assertNull(config.truststore());
        assertEquals("PLAINTEXT", config.kafkaProperties().get("security.protocol"));
        assertFalse(config.kafkaProperties().containsKey("ssl.truststore.location"));
    }

    @Test void requiresTruststoreForSslAndRejectsUnknownProtocol() {
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9093",
                "KAFKA_SECURITY_PROTOCOL", "SASL_SSL")));
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "HTTP")));
    }

    @Test void rejectsNonPositiveEventCountAndNegativeWarmupCount() {
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_EVENT_COUNT", "0")));
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_WARMUP_EVENT_COUNT", "-1")));
    }

    @Test void rejectsReplicationFactorAboveBrokerCount() {
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "KAFKA_BROKER_COUNT", "2",
                "BENCHMARK_REPLICATION_FACTOR", "3")));
    }

    @Test void parsesAndValidatesProcessingMode() {
        Config metadata = Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_PROCESSING_MODE", "metadata"));
        assertEquals(ProcessingMode.METADATA, metadata.processingMode());
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_PROCESSING_MODE", "unknown")));
    }

    @Test void requiresEnoughEvenIterationsToBalanceExecutionOrder() {
        Map<String, String> base = Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_ITERATIONS", "1");
        assertThrows(IllegalArgumentException.class, () -> Config.from(base));
        assertThrows(IllegalArgumentException.class, () -> Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_ITERATIONS", "3")));
        assertEquals(4, Config.from(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")).iterations());
        assertTrue(BenchmarkOrchestrator.streamsFirst(1));
        assertFalse(BenchmarkOrchestrator.streamsFirst(2));
    }
}
