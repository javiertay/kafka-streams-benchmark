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

record FrameEvent(String cameraId, long frameId, String jobId,
                  java.util.List<Detection> metadata, String senderId, long timestamp) {}

record Detection(int bottom, double confidence, String detectionType,
                 int left, int right, int top, long trackId) {}

record FindingPayload(String recordType, String findingType, String cameraId, String jobId,
                      int episodeId, long detectedAtMillis, int slowVehicleCount) {}

record Point(double x, double y) {}
