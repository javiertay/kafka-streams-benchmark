package benchmark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class TraditionalKafkaProcessor implements ProcessorSession {
    private final AtomicInteger consumed = new AtomicInteger();
    private final AtomicInteger published = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final StageMetrics metrics;
    private final Config config;
    private final ResourceSampler resources = new ResourceSampler();
    private final long initialGcCount = ResourceSampler.gcCount();
    private final long initialGcTime = ResourceSampler.gcTime();
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final Thread consumerThread;
    private final Map<String, String> lastEventIds = new HashMap<>();
    private final Map<String, String> aggregates = new HashMap<>();

    TraditionalKafkaProcessor(Config config, WorkerCommand command, StageMetrics metrics) throws Exception {
        this.config = config;
        this.metrics = metrics;
        producer = KafkaSupport.producer(config);
        consumer = KafkaSupport.consumer(config, command.groupId());
        CountDownLatch ready = new CountDownLatch(1);
        consumerThread = Thread.ofPlatform().name("plain-consumer")
                .start(() -> consume(config, command, ready));
        if (!ready.await(config.timeoutSeconds(), TimeUnit.SECONDS))
            throw new IllegalStateException("Traditional consumer did not join its consumer group");
    }

    private void consume(Config config, WorkerCommand command, CountDownLatch ready) {
        consumer.subscribe(List.of(config.inputTopic()));
        long nextCommit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(KafkaSupport.COMMIT_INTERVAL_MS);
        try {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(250));
                ready.countDown();
                for (ConsumerRecord<String, String> record : records) process(config, command, record);
                if (!records.isEmpty() && System.nanoTime() >= nextCommit) {
                    producer.flush();
                    consumer.commitSync();
                    nextCommit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(KafkaSupport.COMMIT_INTERVAL_MS);
                }
            }
        } catch (WakeupException ignored) {
            if (running.get()) throw ignored;
        } finally {
            consumer.close();
        }
    }

    private void process(Config config, WorkerCommand command, ConsumerRecord<String, String> record) {
        long started = System.nanoTime();
        InputEvent input;
        try {
            input = EventCodec.readInput(record.value());
        } catch (IllegalArgumentException ignored) {
            return;
        }
        if (!command.runId().equals(input.runId())) return;
        if (config.processingMode() == ProcessingMode.METADATA && input.sequenceNumber() < 0) {
            publishAggregates(record.partition());
            return;
        }

        metrics.ingested();
        consumed.incrementAndGet();
        if (config.processingMode() == ProcessingMode.METADATA) {
            if (!input.eventId().equals(lastEventIds.put(input.key(), input.eventId()))) {
                String aggregateKey = Workload.aggregateStateKey(
                        record.partition(), record.timestamp(), input.key());
                String previousValue = aggregates.get(aggregateKey);
                OutputEvent previous = previousValue == null ? null : EventCodec.readOutput(previousValue);
                aggregates.put(aggregateKey, EventCodec.write(Workload.aggregate(
                        input, previous, System.currentTimeMillis())));
            }
            metrics.processed(System.nanoTime() - started);
            return;
        }
        OutputEvent output = Workload.transform(input, System.currentTimeMillis());
        publish(output);
        metrics.processed(System.nanoTime() - started);
    }

    private void publishAggregates(int partition) {
        long started = System.nanoTime();
        String prefix = partition + ":";
        Iterator<Map.Entry<String, String>> iterator = aggregates.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> aggregate = iterator.next();
            if (!aggregate.getKey().startsWith(prefix)) continue;
            OutputEvent output = EventCodec.readOutput(aggregate.getValue());
            publishSerialized(output.key(), aggregate.getValue());
            iterator.remove();
        }
        metrics.flushed(System.nanoTime() - started);
    }

    private void publish(OutputEvent output) {
        publishSerialized(output.key(), EventCodec.write(output));
    }

    private void publishSerialized(String key, String value) {
        producer.send(new ProducerRecord<>(config.outputTopic(), key, value),
                (metadata, error) -> {
                    if (error == null) published.incrementAndGet();
                });
    }

    @Override public ProcessorReport stop() throws InterruptedException {
        running.set(false);
        consumer.wakeup();
        consumerThread.join();
        producer.flush();
        producer.close();
        resources.close();
        return new ProcessorReport(consumed.get(), published.get(), metrics.snapshot(), resources.result(), resources.samples(),
                Math.max(0, ResourceSampler.gcCount() - initialGcCount),
                Math.max(0, ResourceSampler.gcTime() - initialGcTime));
    }
}
