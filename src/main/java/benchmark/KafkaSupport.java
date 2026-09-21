package benchmark;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

final class KafkaSupport {
    static final long COMMIT_INTERVAL_MS = 1_000;

    private KafkaSupport() {}

    static void awaitBrokerCount(Config config) throws Exception {
        long deadline = System.nanoTime() + config.timeoutSeconds() * 1_000_000_000L;
        int actual = 0;
        try (AdminClient admin = AdminClient.create(config.kafkaProperties())) {
            while (System.nanoTime() < deadline) {
                actual = admin.describeCluster().nodes().get().size();
                if (actual == config.brokerCount()) return;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("Kafka cluster has " + actual + " brokers; expected "
                + config.brokerCount() + " from KAFKA_BROKER_COUNT");
    }

    static int ensurePartitions(Config config, int requested) throws Exception {
        try (AdminClient admin = AdminClient.create(config.kafkaProperties())) {
            Set<String> existing = admin.listTopics().names().get();
            List<String> benchmarkTopics = List.of(config.inputTopic(), config.outputTopic(), config.partialTopic());
            List<String> missing = benchmarkTopics.stream()
                    .filter(topic -> !existing.contains(topic)).toList();
            if (!missing.isEmpty()) {
                List<NewTopic> topics = missing.stream()
                        .map(topic -> new NewTopic(topic, topic.equals(config.partialTopic()) ? 1 : requested,
                                (short) config.replicationFactor())).toList();
                admin.createTopics(topics).all().get();
            }
            Map<String, org.apache.kafka.clients.admin.TopicDescription> descriptions =
                    admin.describeTopics(List.of(config.inputTopic(), config.outputTopic())).allTopicNames().get();
            int target = Math.max(requested, descriptions.values().stream()
                    .mapToInt(topic -> topic.partitions().size()).max().orElse(requested));
            Map<String, NewPartitions> increases = new java.util.LinkedHashMap<>();
            descriptions.forEach((name, description) -> {
                if (description.partitions().size() < target) increases.put(name, NewPartitions.increaseTo(target));
            });
            if (!increases.isEmpty()) admin.createPartitions(increases).all().get();
            return target;
        }
    }

    static KafkaProducer<String, String> producer(Config config) {
        return new KafkaProducer<>(producerProperties(config));
    }

    static Properties producerProperties(Config config) {
        Properties properties = config.kafkaProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return properties;
    }

    static KafkaConsumer<String, String> consumer(Config config, String group) {
        return new KafkaConsumer<>(consumerProperties(config, group));
    }

    static Properties consumerProperties(Config config, String group) {
        Properties properties = config.kafkaProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.putIfAbsent(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        return properties;
    }

    static Properties streamsProperties(Config config, String applicationId, String runId, int processingThreads) {
        Properties properties = config.kafkaProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, processingThreads);
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, config.processingGuarantee());
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL_MS);
        properties.put(StreamsConfig.STATE_DIR_CONFIG,
                Path.of(System.getProperty("java.io.tmpdir"), runId).toString());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Object maxPollRecords = config.extraKafkaProperties().containsKey(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)
                ? config.extraKafkaProperties().get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG) : 1000;
        properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), maxPollRecords);
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.ACKS_CONFIG), "all");
        properties.put(StreamsConfig.producerPrefix(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), true);
        return properties;
    }

    static void prepareGroupAtEnd(Config config, String group, String topic) {
        try (KafkaConsumer<String, String> consumer = consumer(config, group)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition())).toList();
            consumer.assign(partitions);
            consumer.seekToEnd(partitions);
            Map<TopicPartition, OffsetAndMetadata> offsets = new java.util.LinkedHashMap<>();
            for (TopicPartition partition : partitions) offsets.put(partition, new OffsetAndMetadata(consumer.position(partition)));
            consumer.commitSync(offsets);
        }
    }

    static Map<Integer, Long> endOffsets(Config config, String topic) {
        try (KafkaConsumer<String, String> consumer = consumer(config, "offset-reader-" + java.util.UUID.randomUUID())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition())).toList();
            Map<TopicPartition, Long> offsets = consumer.endOffsets(partitions);
            Map<Integer, Long> result = new java.util.LinkedHashMap<>();
            partitions.forEach(partition -> result.put(partition.partition(), offsets.get(partition)));
            return Map.copyOf(result);
        }
    }

    static void prepareGroupAtOffsets(Config config, String group, String topic, Map<Integer, Long> offsets) {
        try (KafkaConsumer<String, String> consumer = consumer(config, group)) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition())).toList();
            Map<TopicPartition, OffsetAndMetadata> commits = new java.util.LinkedHashMap<>();
            for (TopicPartition partition : partitions) {
                Long offset = offsets.get(partition.partition());
                if (offset == null)
                    throw new IllegalArgumentException("No prepared offset for " + partition);
                commits.put(partition, new OffsetAndMetadata(offset));
            }
            consumer.assign(partitions);
            consumer.commitSync(commits);
        }
    }

    static void awaitGroupMembers(Config config, String group, int expected) throws Exception {
        long deadline = System.nanoTime() + config.timeoutSeconds() * 1_000_000_000L;
        int stableChecks = 0;
        try (AdminClient admin = AdminClient.create(config.kafkaProperties())) {
            while (System.nanoTime() < deadline) {
                var description = admin.describeConsumerGroups(List.of(group)).all().get().get(group);
                if (description != null
                        && description.members().size() == expected
                        && description.groupState() == GroupState.STABLE) stableChecks++;
                else stableChecks = 0;
                if (stableChecks == 4) return;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("Kafka group " + group + " did not reach " + expected + " service members");
    }
}
