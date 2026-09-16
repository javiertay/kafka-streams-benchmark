package benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

final class EventCodec {
    private static final ObjectMapper JSON = new ObjectMapper();

    private EventCodec() {}

    static String write(Object value) {
        try {
            return JSON.writeValueAsString(value);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Cannot serialize benchmark event", exception);
        }
    }

    static InputEvent readInput(String value) {
        return read(value, InputEvent.class);
    }

    static OutputEvent readOutput(String value) {
        return read(value, OutputEvent.class);
    }

    static PartialAggregate readPartial(String value) { return read(value, PartialAggregate.class); }
    static ConsolidatedPayload readConsolidated(String value) { return read(value, ConsolidatedPayload.class); }
    static FrameEvent readFrame(String value) { return read(value, FrameEvent.class); }
    static FindingPayload readFinding(String value) { return read(value, FindingPayload.class); }

    private static <T> T read(String value, Class<T> type) {
        try {
            return JSON.readValue(value, type);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Invalid benchmark JSON", exception);
        }
    }
}
