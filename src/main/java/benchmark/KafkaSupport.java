package benchmark;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

final class KafkaSupport {
    private KafkaSupport() {}

    static int ensurePartitions(Config config, int requested) throws Exception {
        try (AdminClient admin = AdminClient.create(config.kafkaProperties())) {
            Set<String> existing = admin.listTopics().names().get();
            List<String> missing = List.of(config.inputTopic(), config.outputTopic()).stream()
                    .filter(topic -> !existing.contains(topic)).toList();
            if (!missing.isEmpty()) {
                List<NewTopic> topics = missing.stream()
                        .map(topic -> new NewTopic(topic, requested, (short) config.replicationFactor())).toList();
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
        Properties properties = config.kafkaProperties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(properties);
    }

    static KafkaConsumer<String, String> consumer(Config config, String group) {
        Properties properties = config.kafkaProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);
        return new KafkaConsumer<>(properties);
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
}
