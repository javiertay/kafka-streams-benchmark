package benchmark;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;
import java.util.ArrayList;
import java.util.List;
import java.util.SplittableRandom;
import java.util.TreeSet;
import java.util.HashSet;

final class Workload {
    private Workload() {}

    static InputEvent event(String runId, long sequence, int payloadBytes, int uniqueKeys, long seed, long generatedAt) {
        Random random = new Random(seed + sequence);
        byte[] bytes = new byte[payloadBytes];
        random.nextBytes(bytes);
        String payload = Base64.getEncoder().encodeToString(bytes);
        String idSource = seed + ":" + sequence;
        String eventId = UUID.nameUUIDFromBytes(idSource.getBytes(StandardCharsets.UTF_8)).toString();
        return new InputEvent(eventId, runId, sequence, 0,
                "key-" + Math.floorMod(sequence, uniqueKeys), generatedAt, payload);
    }

    static InputEvent event(String runId, long physicalSequence, long identitySequence, long windowIndex,
                            int payloadBytes, int uniqueKeys, long seed, long generatedAt) {
        InputEvent identity = event(runId, identitySequence, payloadBytes, uniqueKeys, seed, generatedAt);
        return new InputEvent(identity.eventId(), runId, physicalSequence, windowIndex,
                identity.key(), generatedAt, identity.payload());
    }

    static List<Integer> inputSchedule(int durationSeconds, int minimum, int maximum, long seed) {
        SplittableRandom random = new SplittableRandom(seed);
        List<Integer> schedule = new ArrayList<>(durationSeconds);
        for (int second = 0; second < durationSeconds; second++) {
            schedule.add(random.nextInt(minimum, maximum + 1));
        }
        return List.copyOf(schedule);
    }

    static OutputEvent transform(InputEvent input, long processedAt) {
        long value = 1125899906842597L;
        String material = input.key() + ':' + input.payload();
        for (int i = 0; i < material.length(); i++) value = 31 * value + material.charAt(i);
        return new OutputEvent(input.eventId(), input.runId(), input.sequenceNumber(), input.key(),
                input.generatedTimestamp(), processedAt, input.payload(), value);
    }

    static final class WindowAccumulator {
        private final HashSet<String> eventIds = new HashSet<>();
        private final TreeSet<String> trackIds = new TreeSet<>();
        private int total;
        private int duplicates;

        void add(InputEvent input) {
            total++;
            if (eventIds.add(input.eventId())) trackIds.add(input.key());
            else duplicates++;
        }

        void merge(PartialAggregate partial) {
            total += partial.totalInputCount();
            duplicates += partial.duplicateCount();
            trackIds.addAll(partial.trackIds());
        }

        PartialAggregate partial(String runId, long window, int partition) {
            return new PartialAggregate(runId, window, partition, List.copyOf(trackIds),
                    total, total - duplicates, duplicates);
        }

        OutputEvent output(String runId, long window, int intervalSeconds,
                           int durationSeconds, long processedAt) {
            ConsolidatedPayload payload = new ConsolidatedPayload(window,
                    window * intervalSeconds * 1_000L,
                    Math.min((window + 1) * intervalSeconds, durationSeconds) * 1_000L,
                    List.copyOf(trackIds), total, total - duplicates, trackIds.size(), duplicates);
            String json = EventCodec.write(payload);
            return new OutputEvent("window-" + window, runId, window, "window-" + window,
                    0, processedAt, json, json.hashCode());
        }
    }

    static final class PartitionedWindows {
        private final java.util.Map<PartitionWindow, WindowAccumulator> windows = new java.util.HashMap<>();

        void add(int partition, InputEvent input) {
            windows.computeIfAbsent(new PartitionWindow(partition, input.windowIndex()),
                    ignored -> new WindowAccumulator()).add(input);
        }

        WindowAccumulator remove(int partition, long window) {
            return windows.remove(new PartitionWindow(partition, window));
        }

        private record PartitionWindow(int partition, long window) {}
    }

}
