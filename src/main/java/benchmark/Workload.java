package benchmark;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;

final class Workload {
    static final int DUPLICATE_EVERY = 5;
    static final long AGGREGATION_WINDOW_MS = 1_000;

    private Workload() {}

    static InputEvent event(String runId, long sequence, int payloadBytes, int uniqueKeys, long seed, long generatedAt) {
        Random random = new Random(seed + sequence);
        byte[] bytes = new byte[payloadBytes];
        random.nextBytes(bytes);
        String payload = Base64.getEncoder().encodeToString(bytes);
        String idSource = seed + ":" + sequence;
        String eventId = UUID.nameUUIDFromBytes(idSource.getBytes(StandardCharsets.UTF_8)).toString();
        return new InputEvent(eventId, runId, sequence, "key-" + Math.floorMod(sequence, uniqueKeys), generatedAt, payload);
    }

    static OutputEvent transform(InputEvent input, long processedAt) {
        long value = 1125899906842597L;
        String material = input.key() + ':' + input.payload();
        for (int i = 0; i < material.length(); i++) value = 31 * value + material.charAt(i);
        return new OutputEvent(input.eventId(), input.runId(), input.sequenceNumber(), input.key(),
                input.generatedTimestamp(), processedAt, input.payload(), value);
    }

    static long metadataSequence(long producedSequence) {
        return producedSequence > 0 && producedSequence % DUPLICATE_EVERY == 0
                ? producedSequence - 1 : producedSequence;
    }

    static long eventTime(long sequence, long eventCount) {
        return sequence * AGGREGATION_WINDOW_MS / eventCount;
    }

    static String aggregateKey(long eventTime, String key) {
        return eventTime / AGGREGATION_WINDOW_MS + ":" + key;
    }

    static String aggregateStateKey(int partition, long eventTime, String key) {
        return partition + ":" + aggregateKey(eventTime, key);
    }

    static OutputEvent aggregate(InputEvent input, OutputEvent previous, long processedAt) {
        long count = previous == null ? 1 : previous.deterministicValue() + 1;
        return new OutputEvent(input.eventId(), input.runId(), input.sequenceNumber(), input.key(),
                input.generatedTimestamp(), processedAt, input.payload(), count);
    }
}
