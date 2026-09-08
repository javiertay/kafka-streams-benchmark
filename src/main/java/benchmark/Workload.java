package benchmark;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;
import java.util.UUID;

final class Workload {
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
}
