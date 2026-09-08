package benchmark;

import com.sun.management.OperatingSystemMXBean;

import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

final class ResourceSampler implements AutoCloseable {
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final List<Double> cpu = new ArrayList<>();
    private final List<Double> ram = new ArrayList<>();
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
        if (load >= 0) cpu.add(load * 100);
        ram.add(residentMemoryMb());
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
