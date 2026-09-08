package benchmark;

record InputEvent(String eventId, String runId, long sequenceNumber, String key,
                  long generatedTimestamp, String payload) {}

record OutputEvent(String eventId, String runId, long sequenceNumber, String key,
                   long generatedTimestamp, long processedTimestamp, String payload,
                   long deterministicValue) {}
