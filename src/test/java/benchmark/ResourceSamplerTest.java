package benchmark;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResourceSamplerTest {
    @Test void aggregatesOnlySimultaneousWorkerSamplesForTruePeak() {
        MetricsSnapshot empty = new MetricsSnapshot(List.of(), List.of(), 0, 0, 0, 0);
        ProcessorReport first = new ProcessorReport(0, 0, empty, new ResourceUsage(50, 90, 100, 100),
                List.of(new ResourceSample(1_000, 90, 100), new ResourceSample(1_100, 10, 100)), 0, 0);
        ProcessorReport second = new ProcessorReport(0, 0, empty, new ResourceUsage(50, 90, 200, 200),
                List.of(new ResourceSample(1_000, 10, 200), new ResourceSample(1_100, 90, 200)), 0, 0);

        ResourceUsage aggregate = ResourceSampler.aggregate(List.of(first, second));

        assertEquals(100, aggregate.averageCpuPercent());
        assertEquals(100, aggregate.peakCpuPercent());
        assertEquals(300, aggregate.averageRamMb());
        assertEquals(300, aggregate.peakRamMb());
    }
}
