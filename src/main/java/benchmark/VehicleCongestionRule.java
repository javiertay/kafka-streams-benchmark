package benchmark;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Stateful vehicle-congestion business rule shared by Kafka Streams and plain Java.
 * One instance belongs to one jobId; transport/framework adapters only decode, call
 * {@link #process(FrameEvent)}, and publish the returned finding when it is non-null.
 */
final class VehicleCongestionRule {
    static final String FINDING_TYPE = "VEHICLE_CONGESTION_VEHICLE_SPEED_DETECTED";

    // Business rule constants: supported classes and the one-second velocity sample period.
    private static final long VELOCITY_SAMPLE_MILLIS = 1_000;
    private static final Set<String> VEHICLES = Set.of("car", "bus", "truck");

    private final Config config;

    // Per-job vehicle history. A track is retained only while it is present inside the ROI.
    private final Map<Long, TrackState> tracks = new HashMap<>();

    // Per-job episode state corresponding to t, absentTimer, and alertActive in the rule definition.
    private long previousTimestamp = -1;
    private double congestionSeconds;
    private double absentSeconds;
    private boolean alertActive;
    private int episode;

    VehicleCongestionRule(Config config) { this.config = config; }

    /**
     * Processes one complete frame for this job.
     *
     * @return a finding only when a new congestion episode starts; otherwise {@code null}
     */
    FindingPayload process(FrameEvent frame) {
        // Stage 1: calculate frame delta. Event time keeps timers independent of configured FPS.
        double delta = previousTimestamp < 0 ? 0 : Math.max(0, (frame.timestamp() - previousTimestamp) / 1000.0);
        previousTimestamp = frame.timestamp();

        // Stage 2: classify the vehicles that are slow and inside the ROI in this frame.
        int slowCount = 0;
        Set<Long> present = new HashSet<>();
        for (Detection detection : frame.metadata()) {
            // Only supported vehicle classes participate in this rule.
            if (!VEHICLES.contains(detection.detectionType().toLowerCase(java.util.Locale.ROOT))) continue;

            // The business position of a detection is its bounding-box bottom centre.
            double x = (detection.left() + detection.right()) / 2.0;
            double y = detection.bottom();
            if (!inside(config.roiPolygon(), x, y)) continue;

            present.add(detection.trackId());
            TrackState track = tracks.computeIfAbsent(detection.trackId(), ignored -> new TrackState());
            track.observe(frame.timestamp(), x, y, config.metersPerPixel());

            // New tracks have no velocity until the first one-second sample and are not counted as slow.
            if (track.velocityKmh != null && track.velocityKmh < config.maxVehicleVelocityKmh()) slowCount++;
        }

        // Stage 3: remove absent vehicles. slowCount never accumulates across frames.
        tracks.keySet().removeIf(id -> !present.contains(id));

        // Stage 4: apply the count gate and continuous congestion/clear timers.
        boolean gate = slowCount >= config.minVehicleCount();
        if (gate) {
            // A recovering gate inside the clear window preserves the active episode.
            absentSeconds = 0;
            congestionSeconds += delta;
            if (!alertActive && congestionSeconds >= config.congestionMinDurationSeconds()) {
                // Stage 5: fire exactly once for this episode; alertActive prevents re-firing.
                alertActive = true;
                episode++;
                return new FindingPayload("FINDING", FINDING_TYPE, frame.cameraId(), frame.jobId(),
                        episode, frame.timestamp(), slowCount);
            }
        } else {
            // Congestion must be continuous, so any failed gate resets the pending timer.
            congestionSeconds = 0;
            if (alertActive) {
                // End the episode only after a continuous absence lasting clearDurationSeconds.
                absentSeconds += delta;
                if (absentSeconds >= config.clearDurationSeconds()) {
                    alertActive = false;
                    absentSeconds = 0;
                }
            }
        }

        // Most frames intentionally produce no output.
        return null;
    }

    private static boolean inside(java.util.List<Point> polygon, double x, double y) {
        // Standard ray-casting point-in-polygon test.
        boolean inside = false;
        for (int i = 0, j = polygon.size() - 1; i < polygon.size(); j = i++) {
            Point a = polygon.get(i), b = polygon.get(j);
            if ((a.y() > y) != (b.y() > y)
                    && x < (b.x() - a.x()) * (y - a.y()) / (b.y() - a.y()) + a.x()) inside = !inside;
        }
        return inside;
    }

    private static final class TrackState {
        // Position and timestamp of the last velocity sample, plus the held velocity estimate.
        long measuredAt = -1;
        double x;
        double y;
        Double velocityKmh;

        void observe(long timestamp, double nextX, double nextY, double metresPerPixel) {
            // A new track needs two positions separated by one second before it has a velocity.
            if (measuredAt < 0) {
                measuredAt = timestamp; x = nextX; y = nextY; return;
            }
            if (timestamp - measuredAt >= VELOCITY_SAMPLE_MILLIS) {
                double seconds = (timestamp - measuredAt) / 1000.0;
                // Euclidean pixel displacement -> metres -> km/h. Hold this value between samples.
                if (seconds > 0) velocityKmh = Math.hypot(nextX - x, nextY - y) * metresPerPixel / seconds * 3.6;
                measuredAt = timestamp; x = nextX; y = nextY;
            }
        }
    }
}
