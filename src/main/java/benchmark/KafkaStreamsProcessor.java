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
    private final ResourceSampler resources = new ResourceSampler();
    private final long initialGcCount = ResourceSampler.gcCount();
    private final long initialGcTime = ResourceSampler.gcTime();
    private final KafkaStreams streams;

    KafkaStreamsProcessor(Config config, WorkerCommand command, StageMetrics metrics) throws Exception {
        this.metrics = metrics;
        streams = new KafkaStreams(buildTopology(config, command),
                KafkaSupport.streamsProperties(config, command.groupId(), command.runId(), 1));
        awaitRunning(config.timeoutSeconds());
    }

    private Topology buildTopology(Config config, WorkerCommand command) {
        StreamsBuilder builder = new StreamsBuilder();
        var input = builder.stream(config.inputTopic(), Consumed.with(Serdes.String(), Serdes.String()));
        if (config.processingMode() == ProcessingMode.METADATA) {
            input.process(() -> metadataPartialProcessor(command))
                    .to(config.partialTopic(), Produced.with(Serdes.String(), Serdes.String()));
            builder.stream(config.partialTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                    .process(() -> metadataGlobalProcessor(config, command))
                    .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        } else {
            input.processValues(() -> transformProcessor(command))
                    .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        }
        return builder.build();
    }

    private FixedKeyProcessor<String, String, String> transformProcessor(WorkerCommand command) {
        return new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<String, String> context;

            @Override public void init(FixedKeyProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override public void process(FixedKeyRecord<String, String> record) {
                long started = System.nanoTime();
                InputEvent input = EventCodec.readInput(record.value());
                if (!command.runId().equals(input.runId())) return;
                metrics.ingested();
                OutputEvent output = Workload.transform(input, System.currentTimeMillis());
                consumed.incrementAndGet();
                context.forward(record.withValue(EventCodec.write(output)));
                forwarded.incrementAndGet();
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
                long started = System.nanoTime();
                InputEvent input = EventCodec.readInput(record.value());
                if (!command.runId().equals(input.runId())) return;
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
                consumed.incrementAndGet();
                windows.computeIfAbsent(input.windowIndex(), ignored -> new Workload.WindowAccumulator()).add(input);
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

    @Override public ProcessorReport stop() {
        streams.close(Duration.ofSeconds(30));
        resources.close();
        return new ProcessorReport(consumed.get(), forwarded.get(), metrics.snapshot(), resources.result(), resources.samples(),
                Math.max(0, ResourceSampler.gcCount() - initialGcCount),
                Math.max(0, ResourceSampler.gcTime() - initialGcTime));
    }
}
