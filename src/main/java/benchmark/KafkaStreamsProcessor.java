package benchmark;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class KafkaStreamsProcessor implements ProcessorSession {
    private final AtomicInteger consumed = new AtomicInteger();
    private final AtomicInteger forwarded = new AtomicInteger();
    private final StageMetrics metrics;
    private volatile ResourceSampler resources;
    private long initialGcCount;
    private long initialGcTime;
    private final KafkaStreams streams;
    private final String runId;
    private final ProcessingGate gate;

    KafkaStreamsProcessor(Config config, WorkerCommand command, StageMetrics metrics, ProcessingGate gate) throws Exception {
        this.metrics = metrics;
        this.runId = command.runId();
        this.gate = gate;
        streams = new KafkaStreams(buildTopology(config, command),
                KafkaSupport.streamsProperties(config, command.groupId(), command.runId(), 1));
        // Pause before start so every instance can join and rebalance while no backlog records are processed.
        // Kafka Streams continues polling and heartbeating a paused topology, unlike blocking a stream callback.
        streams.pause();
        awaitRunning(config.timeoutSeconds());
    }

    private Topology buildTopology(Config config, WorkerCommand command) {
        StreamsBuilder builder = new StreamsBuilder();
        var input = builder.stream(config.inputTopic(), Consumed.with(Serdes.String(), Serdes.String()));
        if (command.benchmarkType() == BenchmarkType.INGESTION_ONLY) {
            input.processValues(() -> ingestionProcessor(command));
        } else if (config.processingMode() == ProcessingMode.METADATA) {
            input.process(() -> metadataPartialProcessor(command))
                    .to(config.partialTopic(), Produced.with(Serdes.String(), Serdes.String()));
            builder.stream(config.partialTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                    .process(() -> metadataGlobalProcessor(config, command))
                    .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        } else if (config.processingMode() == ProcessingMode.VEHICLE_CONGESTION) {
            input.processValues(() -> vehicleProcessor(config, command))
                    .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        } else {
            input.processValues(() -> transformProcessor(command))
                    .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        }
        return builder.build();
    }

    private FixedKeyProcessor<String, String, Void> ingestionProcessor(WorkerCommand command) {
        return new FixedKeyProcessor<>() {
            private boolean started;

            @Override public void init(FixedKeyProcessorContext<String, Void> context) {}

            @Override public void process(FixedKeyRecord<String, String> record) {
                awaitStart();
                if (!isDatasetRecord(record.key(), command.workloadId())) return;
                metrics.ingested();
                consumed.incrementAndGet();
            }

            private void awaitStart() {
                if (!started) {
                    gate.await();
                    started = true;
                }
            }
        };
    }

    private static boolean isDatasetRecord(String key, String workloadId) {
        return key != null && key.startsWith(workloadId + "|")
                && !key.startsWith(workloadId + "|marker-");
    }

    private FixedKeyProcessor<String, String, String> transformProcessor(WorkerCommand command) {
        return new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<String, String> context;

            @Override public void init(FixedKeyProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override public void process(FixedKeyRecord<String, String> record) {
                gate.await();
                long started = System.nanoTime();
                InputEvent input = EventCodec.readInput(record.value());
                if (!command.workloadId().equals(input.runId())) return;
                metrics.ingested();
                OutputEvent output = Workload.transform(input, command.runId(), System.currentTimeMillis());
                context.forward(record.withValue(EventCodec.write(output)));
                forwarded.incrementAndGet();
                consumed.incrementAndGet();
                metrics.processed(System.nanoTime() - started);
            }
        };
    }

    private Processor<String, String, String, String> metadataPartialProcessor(WorkerCommand command) {
        return new Processor<>() {
            private ProcessorContext<String, String> context;
            private final Map<Long, Workload.WindowAccumulator> windows = new HashMap<>();

            @Override public void init(ProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override public void process(Record<String, String> record) {
                gate.await();
                long started = System.nanoTime();
                InputEvent input = EventCodec.readInput(record.value());
                if (!command.workloadId().equals(input.runId())) return;
                if (input.sequenceNumber() < 0) {
                    long flushStarted = System.nanoTime();
                    Workload.WindowAccumulator aggregate = windows.remove(input.windowIndex());
                    if (aggregate == null) aggregate = new Workload.WindowAccumulator();
                    PartialAggregate partial = aggregate.partial(command.runId(), input.windowIndex(),
                            context.recordMetadata().orElseThrow().partition());
                    context.forward(new Record<>(Long.toString(input.windowIndex()),
                            EventCodec.write(partial), record.timestamp()));
                    metrics.flushed(System.nanoTime() - flushStarted);
                    return;
                }
                metrics.ingested();
                windows.computeIfAbsent(input.windowIndex(), ignored -> new Workload.WindowAccumulator()).add(input);
                consumed.incrementAndGet();
                metrics.processed(System.nanoTime() - started);
            }
        };
    }

    private FixedKeyProcessor<String, String, String> vehicleProcessor(Config config, WorkerCommand command) {
        return new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<String, String> context;
            // Input is keyed by jobId, so partition ordering keeps each job's frames in sequence.
            // The business-rule implementation itself is shared with the plain Java processor.
            private final Map<String, VehicleCongestionRule> rules = new HashMap<>();

            @Override public void init(FixedKeyProcessorContext<String, String> context) { this.context = context; }

            @Override public void process(FixedKeyRecord<String, String> record) {
                gate.await();
                long started = System.nanoTime();
                FrameEvent frame;
                try { frame = EventCodec.readFrame(record.value()); }
                catch (IllegalArgumentException ignored) { return; }
                if (!frame.jobId().startsWith(command.workloadId() + ":job-")) return;
                metrics.ingested();
                // One independent state machine per simulated camera-processing job.
                FindingPayload finding = rules.computeIfAbsent(frame.jobId(), ignored -> new VehicleCongestionRule(config))
                        .process(frame);
                if (finding != null) {
                    // Intermediate frames are suppressed; only the episode-start finding is forwarded.
                    OutputEvent output = VehicleWorkload.finding(command.runId(), finding, System.currentTimeMillis());
                    context.forward(record.withValue(EventCodec.write(output)));
                    forwarded.incrementAndGet();
                }
                consumed.incrementAndGet();
                metrics.processed(System.nanoTime() - started);
            }
        };
    }

    private Processor<String, String, String, String> metadataGlobalProcessor(Config config, WorkerCommand command) {
        return new Processor<>() {
            private ProcessorContext<String, String> context;
            private final Map<Long, Workload.WindowAccumulator> windows = new HashMap<>();
            private final Map<Long, Integer> parts = new HashMap<>();

            @Override public void init(ProcessorContext<String, String> context) { this.context = context; }

            @Override public void process(Record<String, String> record) {
                PartialAggregate partial;
                try { partial = EventCodec.readPartial(record.value()); }
                catch (IllegalArgumentException ignored) { return; }
                if (!command.runId().equals(partial.runId())) return;
                long started = System.nanoTime();
                windows.computeIfAbsent(partial.windowIndex(), ignored -> new Workload.WindowAccumulator()).merge(partial);
                int received = parts.merge(partial.windowIndex(), 1, Integer::sum);
                if (received == command.inputPartitions()) {
                    OutputEvent output = windows.remove(partial.windowIndex()).output(command.runId(),
                            partial.windowIndex(), config.outputIntervalSeconds(), command.durationSeconds(),
                            System.currentTimeMillis());
                    parts.remove(partial.windowIndex());
                    context.forward(new Record<>(output.key(), EventCodec.write(output), record.timestamp()));
                    forwarded.incrementAndGet();
                }
                metrics.flushed(System.nanoTime() - started);
            }
        };
    }

    private void awaitRunning(int timeoutSeconds) throws InterruptedException {
        CountDownLatch running = new CountDownLatch(1);
        streams.setStateListener((next, previous) -> {
            if (next == KafkaStreams.State.RUNNING) running.countDown();
        });
        streams.start();
        if (!running.await(timeoutSeconds, TimeUnit.SECONDS))
            throw new IllegalStateException("Kafka Streams did not reach RUNNING state");
    }

    @Override public WorkerProgress progress() {
        return new WorkerProgress(runId, consumed.get(), forwarded.get());
    }

    @Override public synchronized void resume() {
        if (resources != null) return;
        initialGcCount = ResourceSampler.gcCount();
        initialGcTime = ResourceSampler.gcTime();
        resources = new ResourceSampler();
        gate.open();
        streams.resume();
    }

    @Override public ProcessorReport stop() {
        streams.close(Duration.ofSeconds(30));
        ResourceSampler sampler = resources == null ? new ResourceSampler() : resources;
        sampler.close();
        return new ProcessorReport(consumed.get(), forwarded.get(), metrics.snapshot(), sampler.result(), sampler.samples(),
                Math.max(0, ResourceSampler.gcCount() - initialGcCount),
                Math.max(0, ResourceSampler.gcTime() - initialGcTime));
    }
}
