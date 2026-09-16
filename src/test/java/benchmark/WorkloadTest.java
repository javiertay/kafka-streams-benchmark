package benchmark;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class WorkloadTest {
    @Test void generationIsDeterministicAndJsonRoundTrips() {
        InputEvent first = Workload.event("run-a", 7, 32, 5, 42, 1234);
        InputEvent second = Workload.event("run-a", 7, 32, 5, 42, 1234);
        assertEquals(first, second);
        assertEquals(first, EventCodec.readInput(EventCodec.write(first)));
    }

    @Test void bothRunnersShareOneDeterministicTransformation() {
        InputEvent input = Workload.event("run", 3, 12, 2, 9, 100);
        OutputEvent first = Workload.transform(input, 200);
        OutputEvent second = Workload.transform(EventCodec.readInput(EventCodec.write(input)), 200);
        assertEquals(first, second);
        assertEquals(first, EventCodec.readOutput(EventCodec.write(first)));
    }

    @Test void variableInputScheduleIsDeterministicAndBounded() {
        var first = Workload.inputSchedule(300, 10, 500, 42);
        var second = Workload.inputSchedule(300, 10, 500, 42);
        assertEquals(first, second);
        assertEquals(300, first.size());
        assertTrue(first.stream().allMatch(value -> value >= 10 && value <= 500));
        assertTrue(first.stream().distinct().count() > 1);
    }

    @Test void vehicleFramesAreDeterministicVaryDetectionCountsAndRoundTrip() {
        Config config = Config.from(java.util.Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092",
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT",
                "BENCHMARK_PROCESSING_MODE", "vehicle_congestion"));
        FrameEvent frame = VehicleWorkload.frame(config, "run", 0, 7, 1400);
        assertEquals(frame, VehicleWorkload.frame(config, "run", 0, 7, 1400));
        assertEquals(frame, EventCodec.readFrame(EventCodec.write(frame)));
        assertEquals(frame.metadata(), VehicleWorkload.frame(config, "another-run", 0, 7, 9000).metadata());
        assertTrue(java.util.stream.LongStream.range(0, 20)
                .map(i -> VehicleWorkload.frame(config, "run", 0, i, i * 200).metadata().size())
                .distinct().count() > 1);
    }

    @Test void consolidatedPayloadCountsDuplicatesAndMergesPartitionPartials() {
        InputEvent first = Workload.event("run", 0, 0, 2, 8, 10, 42, 100);
        InputEvent duplicate = new InputEvent(first.eventId(), "run", 1, 2,
                first.key(), 101, first.payload());
        InputEvent second = Workload.event("run", 2, 1, 2, 8, 10, 42, 102);
        Workload.WindowAccumulator left = new Workload.WindowAccumulator();
        left.add(first);
        left.add(duplicate);
        Workload.WindowAccumulator right = new Workload.WindowAccumulator();
        right.add(second);
        Workload.WindowAccumulator global = new Workload.WindowAccumulator();
        global.merge(left.partial("run", 2, 0));
        global.merge(right.partial("run", 2, 1));

        ConsolidatedPayload payload = EventCodec.readConsolidated(global.output("run", 2, 5, 15, 200).payload());
        assertEquals(3, payload.totalInputCount());
        assertEquals(2, payload.uniqueEventCount());
        assertEquals(2, payload.uniqueTrackIdCount());
        assertEquals(1, payload.duplicateCount());
        assertEquals(java.util.List.of("key-0", "key-1"), payload.trackIds());
        assertEquals(10_000, payload.windowStartMillis());
        assertEquals(15_000, payload.windowEndMillis());
    }

    @Test void partitionMarkerDoesNotFlushAnotherPartitionsDuplicateState() {
        InputEvent partitionZero = Workload.event("run", 0, 0, 0, 8, 10, 42, 100);
        InputEvent partitionOne = Workload.event("run", 1, 1, 0, 8, 10, 42, 101);
        InputEvent partitionOneDuplicate = new InputEvent(partitionOne.eventId(), "run", 2, 0,
                partitionOne.key(), 102, partitionOne.payload());
        Workload.PartitionedWindows windows = new Workload.PartitionedWindows();
        windows.add(0, partitionZero);
        windows.add(1, partitionOne);

        PartialAggregate first = windows.remove(0, 0).partial("run", 0, 0);
        windows.add(1, partitionOneDuplicate);
        PartialAggregate second = windows.remove(1, 0).partial("run", 0, 1);

        assertEquals(1, first.totalInputCount());
        assertEquals(2, second.totalInputCount());
        assertEquals(1, second.uniqueEventCount());
        assertEquals(1, second.duplicateCount());
    }

    @Test void compactExpectedAccumulatorMatchesFullDeduplication() {
        InputEvent first = Workload.event("run", 0, 0, 0, 8, 10, 42, 100);
        InputEvent duplicate = new InputEvent(first.eventId(), "run", 1, 0,
                first.key(), 101, first.payload());
        InputEvent second = Workload.event("run", 2, 1, 0, 8, 10, 42, 102);
        Workload.WindowAccumulator full = new Workload.WindowAccumulator();
        Workload.ExpectedWindowAccumulator expected = new Workload.ExpectedWindowAccumulator();

        full.add(first);
        full.add(duplicate);
        full.add(second);
        expected.add(first, false);
        expected.add(duplicate, true);
        expected.add(second, false);

        assertEquals(full.output("run", 0, 10, 60, 0).payload(),
                expected.output("run", 0, 10, 60, 0).payload());
    }
}
