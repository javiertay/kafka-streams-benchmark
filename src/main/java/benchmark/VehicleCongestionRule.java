package benchmark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

final class VehicleCongestionRule {
    static final String FINDING_TYPE = "VEHICLE_CONGESTION_VEHICLE_SPEED_DETECTED";
    private static final Set<String> VEHICLES = Set.of("car", "bus", "truck");
    private final Config config;
    private final Map<Long, TrackState> tracks = new HashMap<>();
    private long previousTimestamp = -1;
    private double congestionSeconds;
    private double absentSeconds;
    private boolean alertActive;
    private int episode;

    VehicleCongestionRule(Config config) { this.config = config; }

    FindingPayload process(FrameEvent frame) {
        double delta = previousTimestamp < 0 ? 0 : Math.max(0, (frame.timestamp() - previousTimestamp) / 1000.0);
        previousTimestamp = frame.timestamp();
        int slowCount = 0;
        Set<Long> present = new HashSet<>();
        for (Detection detection : frame.metadata()) {
            if (!VEHICLES.contains(detection.detectionType().toLowerCase(java.util.Locale.ROOT))) continue;
            double x = (detection.left() + detection.right()) / 2.0;
            double y = detection.bottom();
            if (!inside(config.roiPolygon(), x, y)) continue;
            present.add(detection.trackId());
            TrackState track = tracks.computeIfAbsent(detection.trackId(), ignored -> new TrackState());
            track.observe(frame.frameId(), frame.timestamp(), x, y, config.metersPerPixel());
            if (track.velocityKmh != null && track.velocityKmh < config.maxVehicleVelocityKmh()) slowCount++;
        }
        tracks.keySet().removeIf(id -> !present.contains(id));
        boolean gate = slowCount >= config.minVehicleCount();
        if (gate) {
            absentSeconds = 0;
            congestionSeconds += delta;
            if (!alertActive && congestionSeconds >= config.congestionMinDurationSeconds()) {
                alertActive = true;
                episode++;
                return new FindingPayload("FINDING", FINDING_TYPE, frame.cameraId(), frame.jobId(),
                        episode, frame.timestamp(), slowCount);
            }
        } else {
            congestionSeconds = 0;
            if (alertActive) {
                absentSeconds += delta;
                if (absentSeconds >= config.clearDurationSeconds()) {
                    alertActive = false;
                    absentSeconds = 0;
                }
            }
        }
        return null;
    }

    private static boolean inside(java.util.List<Point> polygon, double x, double y) {
        boolean inside = false;
        for (int i = 0, j = polygon.size() - 1; i < polygon.size(); j = i++) {
            Point a = polygon.get(i), b = polygon.get(j);
            if ((a.y() > y) != (b.y() > y)
                    && x < (b.x() - a.x()) * (y - a.y()) / (b.y() - a.y()) + a.x()) inside = !inside;
        }
        return inside;
    }

    private static final class TrackState {
        long measuredFrame = -1;
        long measuredAt;
        double x;
        double y;
        Double velocityKmh;

        void observe(long frame, long timestamp, double nextX, double nextY, double metresPerPixel) {
            if (measuredFrame < 0) {
                measuredFrame = frame; measuredAt = timestamp; x = nextX; y = nextY; return;
            }
            if (frame - measuredFrame >= 5) {
                double seconds = (timestamp - measuredAt) / 1000.0;
                if (seconds > 0) velocityKmh = Math.hypot(nextX - x, nextY - y) * metresPerPixel / seconds * 3.6;
                measuredFrame = frame; measuredAt = timestamp; x = nextX; y = nextY;
            }
        }
    }
}
