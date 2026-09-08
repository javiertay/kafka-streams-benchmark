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
}
