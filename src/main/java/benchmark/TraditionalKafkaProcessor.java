package benchmark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.HashMap;
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
    private final Map<Long, Workload.WindowAccumulator> windows = new HashMap<>();
    private final Map<Long, Workload.WindowAccumulator> globalWindows = new HashMap<>();
    private final Map<Long, Integer> receivedParts = new HashMap<>();

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
        consumer.subscribe(config.processingMode() == ProcessingMode.METADATA
                ? List.of(config.inputTopic(), config.partialTopic()) : List.of(config.inputTopic()));
        long nextCommit = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(KafkaSupport.COMMIT_INTERVAL_MS);
        try {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(250));
                ready.countDown();
                for (ConsumerRecord<String, String> record : records) {
                    if (record.topic().equals(config.partialTopic())) processPartial(config, command, record);
                    else process(config, command, record);
                }
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
            publishPartial(command, input.windowIndex(), record.partition());
            return;
        }

        metrics.ingested();
        consumed.incrementAndGet();
        if (config.processingMode() == ProcessingMode.METADATA) {
            windows.computeIfAbsent(input.windowIndex(), ignored -> new Workload.WindowAccumulator()).add(input);
            metrics.processed(System.nanoTime() - started);
            return;
        }
        OutputEvent output = Workload.transform(input, System.currentTimeMillis());
        publish(output);
        metrics.processed(System.nanoTime() - started);
    }

    private void publishPartial(WorkerCommand command, long window, int partition) {
        long started = System.nanoTime();
        Workload.WindowAccumulator aggregate = windows.remove(window);
        if (aggregate == null) aggregate = new Workload.WindowAccumulator();
        PartialAggregate partial = aggregate.partial(command.runId(), window, partition);
        producer.send(new ProducerRecord<>(config.partialTopic(), Long.toString(window), EventCodec.write(partial)));
        metrics.flushed(System.nanoTime() - started);
    }

    private void processPartial(Config config, WorkerCommand command, ConsumerRecord<String, String> record) {
        PartialAggregate partial;
        try { partial = EventCodec.readPartial(record.value()); }
        catch (IllegalArgumentException ignored) { return; }
        if (!command.runId().equals(partial.runId())) return;
        globalWindows.computeIfAbsent(partial.windowIndex(), ignored -> new Workload.WindowAccumulator()).merge(partial);
        if (receivedParts.merge(partial.windowIndex(), 1, Integer::sum) == command.inputPartitions()) {
            OutputEvent output = globalWindows.remove(partial.windowIndex()).output(command.runId(),
                    partial.windowIndex(), config.outputIntervalSeconds(), command.durationSeconds(),
                    System.currentTimeMillis());
            receivedParts.remove(partial.windowIndex());
            publish(output);
        }
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
