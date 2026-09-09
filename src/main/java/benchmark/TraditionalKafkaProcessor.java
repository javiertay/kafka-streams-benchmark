package benchmark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class TraditionalKafkaProcessor implements ProcessorSession {
    private final AtomicInteger consumed = new AtomicInteger();
    private final AtomicInteger published = new AtomicInteger();
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final StageMetrics metrics;
    private final ResourceSampler resources = new ResourceSampler();
    private final long initialGcCount = ResourceSampler.gcCount();
    private final long initialGcTime = ResourceSampler.gcTime();
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;
    private final Thread consumerThread;

    TraditionalKafkaProcessor(Config config, WorkerCommand command, StageMetrics metrics) throws Exception {
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
        try {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(250));
                ready.countDown();
                for (ConsumerRecord<String, String> record : records) process(config, command, record);
            }
        } catch (WakeupException ignored) {
            if (running.get()) throw ignored;
        } finally {
            consumer.close();
        }
    }

    private void process(Config config, WorkerCommand command, ConsumerRecord<String, String> record) {
        InputEvent input;
        try {
            input = EventCodec.readInput(record.value());
        } catch (IllegalArgumentException ignored) {
            return;
        }
        if (!command.runId().equals(input.runId())) return;

        metrics.ingested(Math.max(0, (System.currentTimeMillis() - record.timestamp()) * 1_000_000));
        long started = System.nanoTime();
        OutputEvent output = Workload.transform(input, System.currentTimeMillis());
        metrics.processed(System.nanoTime() - started);
        consumed.incrementAndGet();
        producer.send(new ProducerRecord<>(config.outputTopic(), output.key(), EventCodec.write(output)),
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
        return new ProcessorReport(consumed.get(), published.get(), metrics.snapshot(), resources.result(),
                Math.max(0, ResourceSampler.gcCount() - initialGcCount),
                Math.max(0, ResourceSampler.gcTime() - initialGcTime));
    }
}
