package benchmark;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class VehicleCongestionRuleTest {
    @Test void filtersTypesUsesBottomCentreAndFiresOnlyOnceUntilCleared() {
        Config config = config(0, 1);
        VehicleCongestionRule rule = new VehicleCongestionRule(config);
        assertNull(rule.process(frame(0, 0, vehicles(100, 100))));
        FindingPayload first = rule.process(frame(5, 1000, vehicles(100, 100)));
        assertNotNull(first);
        assertEquals(VehicleCongestionRule.FINDING_TYPE, first.findingType());
        assertEquals(2, first.slowVehicleCount());
        assertNull(rule.process(frame(10, 2000, vehicles(100, 100))));

        assertNull(rule.process(frame(11, 2200, List.of())));
        assertNull(rule.process(frame(12, 3200, List.of())));
        assertNull(rule.process(frame(13, 3400, vehicles(100, 100))));
        FindingPayload second = rule.process(frame(18, 4400, vehicles(100, 100)));
        assertNotNull(second);
        assertEquals(2, second.episodeId());
    }

    @Test void continuousGateMustReachConfiguredDuration() {
        VehicleCongestionRule rule = new VehicleCongestionRule(config(2, 1));
        assertNull(rule.process(frame(0, 0, vehicles(100, 100))));
        assertNull(rule.process(frame(5, 1000, vehicles(100, 100))));
        assertNotNull(rule.process(frame(10, 2000, vehicles(100, 100))));
    }

    private static Config config(int minimumDuration, int clearDuration) {
        return Config.from(Map.ofEntries(
                Map.entry("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
                Map.entry("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
                Map.entry("BENCHMARK_PROCESSING_MODE", "vehicle_congestion"),
                Map.entry("BENCHMARK_MIN_VEHICLE_COUNT", "2"),
                Map.entry("BENCHMARK_MAX_DETECTIONS_PER_FRAME", "5"),
                Map.entry("BENCHMARK_CONGESTION_MIN_DURATION_SECONDS", Integer.toString(minimumDuration)),
                Map.entry("BENCHMARK_CLEAR_DURATION_SECONDS", Integer.toString(clearDuration))));
    }

    private static FrameEvent frame(long id, long timestamp, List<Detection> detections) {
        return new FrameEvent("camera", id, "run:job-0", detections, "sender", timestamp);
    }

    private static List<Detection> vehicles(int firstX, int secondX) {
        return List.of(
                new Detection(100, .9, "car", firstX - 10, firstX + 10, 50, 1),
                new Detection(200, .9, "bus", secondX - 10, secondX + 10, 150, 2),
                new Detection(100, .9, "cone", 90, 110, 50, 3),
                new Detection(2000, .9, "truck", 90, 110, 1950, 4));
    }
}
