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
    final long startedNanos = System.nanoTime();
    final ConcurrentLinkedQueue<Long> ingestion = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> processing = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> publishing = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<Long> endToEnd = new ConcurrentLinkedQueue<>();
    final AtomicLong firstIngestNanos = new AtomicLong();
    final AtomicLong lastIngestNanos = new AtomicLong();
    final AtomicLong firstProcessNanos = new AtomicLong();
    final AtomicLong lastProcessNanos = new AtomicLong();
    final AtomicLong firstPublishNanos = new AtomicLong();
    final AtomicLong lastPublishNanos = new AtomicLong();

    void ingested(long latency) { ingestion.add(latency); mark(firstIngestNanos, lastIngestNanos); }
    void processed(long latency) { processing.add(latency); mark(firstProcessNanos, lastProcessNanos); }
    void published(long latency) { publishing.add(latency); mark(firstPublishNanos, lastPublishNanos); }

    double throughput(AtomicLong first, AtomicLong last, int count) {
        long duration = last.get() - first.get();
        return duration <= 0 ? 0 : count * 1_000_000_000.0 / duration;
    }

    private static void mark(AtomicLong first, AtomicLong last) {
        long now = System.nanoTime();
        first.compareAndSet(0, now);
        last.set(now);
    }
}
