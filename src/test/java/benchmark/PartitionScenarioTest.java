package benchmark;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PartitionScenarioTest {
    @Test void skipsOnlyWhenRetainedTopicAlreadyExceedsRequest() {
        assertTrue(BenchmarkOrchestrator.shouldSkip(3, 6));
        assertFalse(BenchmarkOrchestrator.shouldSkip(6, 6));
    }

    @Test void excludesServiceCountsThatCannotReceivePartitions() {
        assertTrue(BenchmarkOrchestrator.isMeaningfulScalingScenario(3, 3));
        assertTrue(BenchmarkOrchestrator.isMeaningfulScalingScenario(6, 3));
        assertFalse(BenchmarkOrchestrator.isMeaningfulScalingScenario(1, 3));
        assertFalse(BenchmarkOrchestrator.isMeaningfulScalingScenario(3, 6));
    }
}
