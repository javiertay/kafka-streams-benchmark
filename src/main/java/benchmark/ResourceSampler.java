package benchmark;

import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

final class ResourceSampler implements AutoCloseable {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<Double> cpu = new ArrayList<>();
    private final List<Double> ram = new ArrayList<>();
    private final List<ResourceSample> samples = new ArrayList<>();
    private final Thread thread;

    ResourceSampler() {
        thread = Thread.ofPlatform().name("resource-sampler").start(() -> {
            while (running.get()) {
                sample();
                try { Thread.sleep(100); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); break; }
            }
        });
    }

    private synchronized void sample() {
        var bean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        double load = bean.getProcessCpuLoad();
        double memory = residentMemoryMb();
        if (load >= 0) {
            double percent = load * 100;
            cpu.add(percent);
            samples.add(new ResourceSample(System.currentTimeMillis(), percent, memory));
        }
        ram.add(memory);
    }

    private double residentMemoryMb() {
        Path status = Path.of("/proc/self/status");
        if (Files.isReadable(status)) {
            try {
                return Files.readAllLines(status).stream()
                        .filter(line -> line.startsWith("VmRSS:"))
                        .map(line -> line.replaceAll("[^0-9]", ""))
                        .mapToLong(Long::parseLong).findFirst().orElse(0) / 1024.0;
            } catch (IOException ignored) { }
        }
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed() / 1024.0 / 1024.0;
    }

    synchronized ResourceUsage result() {
        return new ResourceUsage(cpu.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                cpu.stream().mapToDouble(Double::doubleValue).max().orElse(0),
                ram.stream().mapToDouble(Double::doubleValue).average().orElse(0),
                ram.stream().mapToDouble(Double::doubleValue).max().orElse(0));
    }

    synchronized List<ResourceSample> samples() { return List.copyOf(samples); }

    static ResourceUsage aggregate(List<ProcessorReport> reports) {
        if (reports.isEmpty()) return new ResourceUsage(0, 0, 0, 0);
        Map<Long, double[]> buckets = new HashMap<>();
        for (ProcessorReport report : reports) {
            Map<Long, ResourceSample> workerBuckets = new HashMap<>();
            for (ResourceSample sample : report.resourceSamples()) {
                workerBuckets.put(sample.epochMillis() / 100, sample);
            }
            for (Map.Entry<Long, ResourceSample> entry : workerBuckets.entrySet()) {
                long bucket = entry.getKey();
                ResourceSample sample = entry.getValue();
                double[] totals = buckets.computeIfAbsent(bucket, ignored -> new double[3]);
                totals[0] += sample.cpuPercent();
                totals[1] += sample.ramMb();
                totals[2]++;
            }
        }
        List<double[]> simultaneous = buckets.values().stream()
                .filter(values -> values[2] == reports.size()).toList();
        if (simultaneous.isEmpty()) {
            return new ResourceUsage(
                    reports.stream().mapToDouble(r -> r.resources().averageCpuPercent()).sum(),
                    reports.stream().mapToDouble(r -> r.resources().peakCpuPercent()).sum(),
                    reports.stream().mapToDouble(r -> r.resources().averageRamMb()).sum(),
                    reports.stream().mapToDouble(r -> r.resources().peakRamMb()).sum());
        }
        return new ResourceUsage(
                simultaneous.stream().mapToDouble(values -> values[0]).average().orElse(0),
                simultaneous.stream().mapToDouble(values -> values[0]).max().orElse(0),
                simultaneous.stream().mapToDouble(values -> values[1]).average().orElse(0),
                simultaneous.stream().mapToDouble(values -> values[1]).max().orElse(0));
    }

    @Override public void close() {
        running.set(false);
        thread.interrupt();
        try { thread.join(); } catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
    }

    static long gcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(GarbageCollectorMXBean::getCollectionCount)
                .filter(value -> value >= 0).sum();
    }

    static long gcTime() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream().mapToLong(GarbageCollectorMXBean::getCollectionTime)
                .filter(value -> value >= 0).sum();
    }
}

record ResourceSample(long epochMillis, double cpuPercent, double ramMb) {}
