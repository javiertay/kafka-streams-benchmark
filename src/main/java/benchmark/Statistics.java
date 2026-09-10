package benchmark;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

record Latency(double p50Ms, double p95Ms, double p99Ms, double maxMs) {
    static Latency empty() { return new Latency(0, 0, 0, 0); }
}

final class Statistics {
    private Statistics() {}

    static Latency latency(Collection<Long> nanoseconds) {
        if (nanoseconds.isEmpty()) return Latency.empty();
        long[] values = nanoseconds.stream().mapToLong(Long::longValue).sorted().toArray();
        return new Latency(ms(percentile(values, .50)), ms(percentile(values, .95)),
                ms(percentile(values, .99)), ms(values[values.length - 1]));
    }

    static double median(Collection<Double> values) {
        if (values.isEmpty()) return 0;
        double[] sorted = values.stream().mapToDouble(Double::doubleValue).sorted().toArray();
        int middle = sorted.length / 2;
        return sorted.length % 2 == 0 ? (sorted[middle - 1] + sorted[middle]) / 2 : sorted[middle];
    }

    static <T> T medianBy(Collection<T> values, java.util.function.ToDoubleFunction<T> extractor) {
        List<T> sorted = values.stream().sorted(Comparator.comparingDouble(extractor)).toList();
        return sorted.get((sorted.size() - 1) / 2);
    }

    private static long percentile(long[] sorted, double percentile) {
        int index = Math.max(0, (int) Math.ceil(percentile * sorted.length) - 1);
        return sorted[index];
    }

    private static double ms(long nanoseconds) { return nanoseconds / 1_000_000.0; }
}

final class StageMetrics {
    private static final int MAX_SAMPLES_PER_STAGE = 100_000;
    final ConcurrentLinkedQueue<Long> processing = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> flushing = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> publishing = new ConcurrentLinkedQueue<>();
    final AtomicLong firstIngestNanos = new AtomicLong();
    final AtomicLong lastIngestNanos = new AtomicLong();
    final AtomicLong firstProcessNanos = new AtomicLong();
    final AtomicLong lastProcessNanos = new AtomicLong();
    final AtomicLong firstPublishNanos = new AtomicLong();
    final AtomicLong lastPublishNanos = new AtomicLong();
    private final long sampleEvery;
    private final AtomicLong processingSeen = new AtomicLong();
    private final AtomicLong publishingSeen = new AtomicLong();

    StageMetrics(long estimatedEvents) {
        sampleEvery = Math.max(1, (estimatedEvents + MAX_SAMPLES_PER_STAGE - 1) / MAX_SAMPLES_PER_STAGE);
    }

    void ingested() { mark(firstIngestNanos, lastIngestNanos); }
    void processed(long latency) { sample(processing, processingSeen, latency); mark(firstProcessNanos, lastProcessNanos); }
    void flushed(long latency) { flushing.add(latency); }
    void published(long latency) { sample(publishing, publishingSeen, latency); markMonotonic(firstPublishNanos, lastPublishNanos); }

    MetricsSnapshot snapshot() {
        return new MetricsSnapshot(List.copyOf(processing), List.copyOf(flushing),
                firstIngestNanos.get(), lastIngestNanos.get(), firstProcessNanos.get(), lastProcessNanos.get());
    }

    void merge(MetricsSnapshot snapshot) {
        processing.addAll(snapshot.processing());
        flushing.addAll(snapshot.flushing());
        mergeRange(firstIngestNanos, lastIngestNanos, snapshot.firstIngest(), snapshot.lastIngest());
        mergeRange(firstProcessNanos, lastProcessNanos, snapshot.firstProcess(), snapshot.lastProcess());
    }

    double throughput(AtomicLong first, AtomicLong last, int count) {
        long duration = last.get() - first.get();
        return duration <= 0 || count < 2 ? 0 : (count - 1) * 1_000_000_000.0 / duration;
    }

    double elapsedSeconds(AtomicLong first, AtomicLong last) {
        return Math.max(0, last.get() - first.get()) / 1_000_000_000.0;
    }

    private static void mark(AtomicLong first, AtomicLong last) {
        long now = System.currentTimeMillis() * 1_000_000;
        first.compareAndSet(0, now);
        last.set(now);
    }

    private static void markMonotonic(AtomicLong first, AtomicLong last) {
        long now = System.nanoTime();
        first.compareAndSet(0, now);
        last.set(now);
    }

    private void sample(ConcurrentLinkedQueue<Long> values, AtomicLong seen, long latency) {
        if (seen.getAndIncrement() % sampleEvery == 0) values.add(latency);
    }

    private static void mergeRange(AtomicLong first, AtomicLong last, long otherFirst, long otherLast) {
        if (otherFirst > 0) first.updateAndGet(value -> value == 0 ? otherFirst : Math.min(value, otherFirst));
        if (otherLast > 0) last.updateAndGet(value -> Math.max(value, otherLast));
    }
}

record MetricsSnapshot(List<Long> processing, List<Long> flushing,
                       long firstIngest, long lastIngest, long firstProcess, long lastProcess) {}
