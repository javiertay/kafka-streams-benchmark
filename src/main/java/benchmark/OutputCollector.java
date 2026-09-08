package benchmark;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class OutputCollector implements AutoCloseable {
    private final KafkaConsumer<String, String> consumer;
    private final String runId;
    private final int expected;
    private final StageMetrics metrics;
    private final BitSet seen;
    private final CountDownLatch complete = new CountDownLatch(1);
    private final CountDownLatch ready = new CountDownLatch(1);
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger observed = new AtomicInteger();
    private final AtomicInteger duplicates = new AtomicInteger();
    private final AtomicInteger unexpected = new AtomicInteger();
    private final Thread thread;

    OutputCollector(Config config, String runId, int expected, StageMetrics metrics) throws InterruptedException {
        this.consumer = KafkaSupport.consumer(config, "benchmark-observer-" + runId);
        this.runId = runId;
        this.expected = expected;
        this.metrics = metrics;
        this.seen = new BitSet(expected);
        thread = Thread.ofPlatform().name("output-observer").start(() -> collect(config.outputTopic()));
        if (!ready.await(config.timeoutSeconds(), java.util.concurrent.TimeUnit.SECONDS)) {
            close();
            throw new IllegalStateException("Output observer could not obtain topic assignments");
        }
    }

    private void collect(String topic) {
        try {
            consumer.subscribe(java.util.List.of(topic));
            while (running.get() && consumer.assignment().isEmpty()) consumer.poll(Duration.ofMillis(100));
            consumer.seekToEnd(consumer.assignment());
            ready.countDown();
            while (running.get() && observed.get() < expected) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                    OutputEvent output;
                    try { output = EventCodec.readOutput(record.value()); }
                    catch (IllegalArgumentException invalid) { continue; }
                    if (!runId.equals(output.runId())) continue;
                    long sequence = output.sequenceNumber();
                    if (sequence < 0 || sequence >= expected) { unexpected.incrementAndGet(); continue; }
                    synchronized (seen) {
                        if (seen.get((int) sequence)) { duplicates.incrementAndGet(); continue; }
                        seen.set((int) sequence);
                    }
                    observed.incrementAndGet();
                    long nowEpochMillis = System.currentTimeMillis();
                    metrics.published(Math.max(0, (nowEpochMillis - output.processedTimestamp()) * 1_000_000));
                    metrics.endToEnd.add(Math.max(0, (nowEpochMillis - output.generatedTimestamp()) * 1_000_000));
                }
            }
        } catch (org.apache.kafka.common.errors.WakeupException ignored) {
            if (running.get()) throw ignored;
        } finally { ready.countDown(); complete.countDown(); }
    }

    boolean await(Duration timeout) throws InterruptedException { return complete.await(timeout.toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS); }

    Validation validation(int sent, int consumed, int published) {
        int found = observed.get();
        return new Validation(expected, sent, consumed, published, found,
                Math.max(0, expected - found), duplicates.get(), unexpected.get());
    }

    @Override public void close() {
        running.set(false);
        consumer.wakeup();
        try { thread.join(); } catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
        consumer.close();
    }
}
