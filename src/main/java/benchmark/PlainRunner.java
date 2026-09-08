package benchmark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class PlainRunner {
    BenchmarkResult run(Config config, int eventCount, int requestedPartitions, int actualPartitions,
                        int processingThreads, long inputRate, int iteration, long seed) throws Exception {
        String runId = RunSupport.runId("plain-java");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger published = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);
        List<KafkaConsumer<String, String>> consumers = java.util.Collections.synchronizedList(new ArrayList<>());
        String group = "plain-" + runId;
        KafkaSupport.prepareGroupAtEnd(config, group, config.inputTopic());
        long gcCount = ResourceSampler.gcCount();
        long gcTime = ResourceSampler.gcTime();
        StageMetrics metrics = new StageMetrics();
        try (ResourceSampler sampler = new ResourceSampler();
             KafkaProducer<String, String> producer = KafkaSupport.producer(config);
             OutputCollector collector = new OutputCollector(config, runId, eventCount, metrics)) {
            CountDownLatch ready = new CountDownLatch(processingThreads);
            RunContext context = new RunContext(runId, group, eventCount, metrics, producer, running,
                    consumed, published, consumers, ready);
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < processingThreads; i++) {
                workers.add(startWorker(i, config, context));
            }
            ready.await();
            Generation generation = RunSupport.generate(config, runId, eventCount, inputRate, seed);
            boolean completed = collector.await(Duration.ofSeconds(config.timeoutSeconds()));
            running.set(false);
            synchronized (consumers) { consumers.forEach(KafkaConsumer::wakeup); }
            for (Thread worker : workers) worker.join();
            producer.flush();
            long finished = System.nanoTime();
            if (!completed) System.err.println("Plain Java run timed out: " + runId);
            Validation validation = collector.validation(generation.sent(), consumed.get(), published.get());
            sampler.close();
            return RunSupport.result(config, "Plain Java", runId, iteration, eventCount, requestedPartitions,
                    actualPartitions, processingThreads, inputRate, generation, metrics, finished,
                    sampler.result(), validation, gcCount, gcTime);
        }
    }

    private static Thread startWorker(int workerNumber, Config config, RunContext context) {
        return Thread.ofPlatform().name("plain-worker-" + workerNumber)
                .start(() -> consumeAndPublish(config, context));
    }

    private static void consumeAndPublish(Config config, RunContext context) {
        KafkaConsumer<String, String> consumer = KafkaSupport.consumer(config, context.groupId());
        context.consumers().add(consumer);
        consumer.subscribe(List.of(config.inputTopic()));
        context.ready().countDown();
        try {
            while (context.running().get() && context.consumed().get() < context.expectedEvents()) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    processAndPublish(config, context, record);
                }
            }
        } catch (WakeupException ignored) {
            if (context.running().get()) throw ignored;
        } finally {
            consumer.close();
        }
    }

    private static void processAndPublish(Config config, RunContext context,
                                          ConsumerRecord<String, String> record) {
        long processingStarted = System.nanoTime();
        InputEvent input;
        try {
            input = EventCodec.readInput(record.value());
        } catch (IllegalArgumentException invalidJson) {
            return;
        }
        if (!context.runId().equals(input.runId())) return;

        context.metrics().ingested(Math.max(0,
                (System.currentTimeMillis() - record.timestamp()) * 1_000_000));
        OutputEvent output = Workload.transform(input, System.currentTimeMillis());
        String outputJson = EventCodec.write(output);
        context.metrics().processed(System.nanoTime() - processingStarted);
        context.consumed().incrementAndGet();
        context.producer().send(new ProducerRecord<>(config.outputTopic(), output.key(), outputJson),
                (metadata, error) -> {
                    if (error == null) context.published().incrementAndGet();
                });
    }

    private record RunContext(
            String runId,
            String groupId,
            int expectedEvents,
            StageMetrics metrics,
            KafkaProducer<String, String> producer,
            AtomicBoolean running,
            AtomicInteger consumed,
            AtomicInteger published,
            List<KafkaConsumer<String, String>> consumers,
            CountDownLatch ready) {}
}
