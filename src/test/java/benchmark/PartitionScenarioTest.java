package benchmark;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PartitionScenarioTest {
    @Test void skipsOnlyWhenRetainedTopicAlreadyExceedsRequest() {
        assertTrue(BenchmarkOrchestrator.shouldSkip(3, 6));
        assertFalse(BenchmarkOrchestrator.shouldSkip(6, 6));
    }
}
