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
                        int iteration, long seed) throws Exception {
        String runId = RunSupport.runId("plain-java");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger published = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);
        List<KafkaConsumer<String, String>> consumers = java.util.Collections.synchronizedList(new ArrayList<>());
        String group = "plain-" + runId;
        KafkaSupport.prepareGroupAtEnd(config, group, config.inputTopic());
        long gcCount = ResourceSampler.gcCount();
        long gcTime = ResourceSampler.gcTime();
        Generation generation = RunSupport.generate(config, runId, eventCount, seed);
        StageMetrics metrics = new StageMetrics();
        try (ResourceSampler sampler = new ResourceSampler();
             KafkaProducer<String, String> producer = KafkaSupport.producer(config);
             OutputCollector collector = new OutputCollector(config, runId, eventCount, metrics)) {
            CountDownLatch ready = new CountDownLatch(config.threads());
            List<Thread> workers = new ArrayList<>();
            for (int i = 0; i < config.threads(); i++) {
                Thread worker = Thread.ofPlatform().name("plain-worker-" + i).start(() -> {
                    KafkaConsumer<String, String> consumer = KafkaSupport.consumer(config, group);
                    consumers.add(consumer);
                    consumer.subscribe(List.of(config.inputTopic()));
                    ready.countDown();
                    try {
                        while (running.get() && consumed.get() < eventCount) {
                            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                                long processStarted = System.nanoTime();
                                InputEvent input;
                                try { input = EventCodec.readInput(record.value()); }
                                catch (IllegalArgumentException invalid) { continue; }
                                if (!runId.equals(input.runId())) continue;
                                metrics.ingested(Math.max(0, (System.currentTimeMillis() - record.timestamp()) * 1_000_000));
                                OutputEvent output = Workload.transform(input, System.currentTimeMillis());
                                String outputJson = EventCodec.write(output);
                                metrics.processed(System.nanoTime() - processStarted);
                                consumed.incrementAndGet();
                                producer.send(new ProducerRecord<>(config.outputTopic(), output.key(), outputJson),
                                        (metadata, error) -> { if (error == null) published.incrementAndGet(); });
                            }
                        }
                    } catch (WakeupException ignored) {
                        if (running.get()) throw ignored;
                    } finally { consumer.close(); }
                });
                workers.add(worker);
            }
            ready.await();
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
                    actualPartitions, generation, metrics, finished, sampler.result(), validation, gcCount, gcTime);
        }
    }
}
