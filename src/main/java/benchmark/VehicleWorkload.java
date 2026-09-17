package benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;

final class VehicleWorkload {
    private static final int NORMAL_PIXELS_PER_SECOND = 100;

    private VehicleWorkload() {}

    static int eventCount(Config config, int durationSeconds) {
        return Math.multiplyExact(Math.multiplyExact(durationSeconds, config.framesPerSecond()), config.simulatedJobs());
    }

    static FrameEvent frame(Config config, String runId, int job, long frameId, long timestamp) {
        long seed = config.workloadSeed() ^ (job * 0x9e3779b97f4a7c15L) ^ frameId;
        SplittableRandom random = new SplittableRandom(seed);
        int requested = random.nextInt(config.minDetectionsPerFrame(), config.maxDetectionsPerFrame() + 1);
        int second = (int) (frameId / config.framesPerSecond());
        int normalSeconds = 3;
        int congestedSeconds = Math.max(5, config.congestionMinDurationSeconds() + 2);
        int cycleSeconds = normalSeconds + congestedSeconds + config.clearDurationSeconds() + 2;
        int phase = second % cycleSeconds;
        boolean congested = phase >= normalSeconds && phase < normalSeconds + congestedSeconds;
        int count = congested ? Math.max(requested, config.minVehicleCount()) : requested;
        List<Detection> detections = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            boolean vehicle = congested && index < config.minVehicleCount() || index % 5 != 4;
            String type = vehicle ? switch (index % 3) { case 0 -> "car"; case 1 -> "bus"; default -> "truck"; } : "cone";
            int baseX = 100 + Math.floorMod(index * 71 + job * 37, 1700);
            long movement = frameId * NORMAL_PIXELS_PER_SECOND / config.framesPerSecond();
            int x = congested ? baseX : 50 + (int) Math.floorMod(baseX + movement, 1800L);
            int bottom = 150 + Math.floorMod(index * 43, 800);
            detections.add(new Detection(bottom, 0.5 + random.nextDouble() * 0.49, type,
                    x - 20, x + 20, bottom - 50, index));
        }
        String camera = "camera-" + job;
        String jobId = runId + ":job-" + job;
        return new FrameEvent(camera, frameId, jobId, List.copyOf(detections), "benchmark-sender", timestamp);
    }

    static OutputEvent finding(String runId, FindingPayload finding, long processedAt) {
        int job = Integer.parseInt(finding.jobId().substring(finding.jobId().lastIndexOf("job-") + 4));
        long sequence = job * 10_000L + finding.episodeId();
        String payload = EventCodec.write(finding);
        return new OutputEvent("finding-" + job + '-' + finding.episodeId(), runId, sequence,
                finding.jobId(), finding.detectedAtMillis(), processedAt, payload, payload.hashCode());
    }
}
