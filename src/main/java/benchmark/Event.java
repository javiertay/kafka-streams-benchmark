package benchmark;

record InputEvent(String eventId, String runId, long sequenceNumber, long windowIndex, String key,
                  long generatedTimestamp, String payload) {}

record ConsolidatedPayload(long windowIndex, long windowStartMillis, long windowEndMillis,
                           java.util.List<String> trackIds, int totalInputCount,
                           int uniqueEventCount, int uniqueTrackIdCount, int duplicateCount) {}

record PartialAggregate(String runId, long windowIndex, int sourcePartition,
                        java.util.List<String> trackIds, int totalInputCount,
                        int uniqueEventCount, int duplicateCount) {}

record OutputEvent(String eventId, String runId, long sequenceNumber, String key,
                   long generatedTimestamp, long processedTimestamp, String payload,
                   long deterministicValue) {}
