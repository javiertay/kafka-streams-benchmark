package benchmark;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResourceSamplerTest {
    @Test void calculatesParallelActiveProcessingFromBusiestWorker() {
        MetricsSnapshot firstMetrics = new MetricsSnapshot(List.of(), List.of(), 0, 0, 0, 0,
                100, 1_000_000_000L);
        MetricsSnapshot secondMetrics = new MetricsSnapshot(List.of(), List.of(), 0, 0, 0, 0,
                200, 2_000_000_000L);
        ResourceUsage none = new ResourceUsage(0, 0, 0, 0);
        ActiveProcessing active = DistributedRunner.activeProcessing(List.of(
                new ProcessorReport(100, 0, firstMetrics, none, List.of(), 0, 0),
                new ProcessorReport(200, 0, secondMetrics, none, List.of(), 0, 0)), 300);

        assertEquals(2, active.seconds());
        assertEquals(150, active.throughput());
        assertEquals(10, active.meanMs());
    }

    @Test void combinesLiveWorkerProgressForDrainChecks() {
        WorkerProgress combined = WorkerProgress.combine("run", List.of(
                new WorkerProgress("run", 120, 2),
                new WorkerProgress("run", 80, 3)));

        assertEquals("run", combined.runId());
        assertEquals(200, combined.consumed());
        assertEquals(5, combined.published());
    }

    @Test void aggregatesOnlySimultaneousWorkerSamplesForTruePeak() {
        MetricsSnapshot empty = new MetricsSnapshot(List.of(), List.of(), 0, 0, 0, 0, 0, 0);
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
