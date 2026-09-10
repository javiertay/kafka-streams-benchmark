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

    @Test void metadataWorkloadDuplicatesEveryFifthEventAndAggregatesCounts() {
        assertEquals(4, Workload.metadataSequence(4));
        assertEquals(4, Workload.metadataSequence(5));
        assertEquals(6, Workload.metadataSequence(6));
        assertEquals("2:key-7", Workload.aggregateKey(2_500, "key-7"));
        assertEquals(0, Workload.eventTime(0, 500_000));
        assertEquals(999, Workload.eventTime(499_999, 500_000));

        InputEvent input = Workload.event("run", 4, 4, 10, 42, 100);
        OutputEvent first = Workload.aggregate(input, null, 200);
        OutputEvent second = Workload.aggregate(input, first, 300);
        assertEquals(1, first.deterministicValue());
        assertEquals(2, second.deterministicValue());
    }
}
