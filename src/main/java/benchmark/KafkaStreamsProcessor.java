package benchmark;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
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
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class KafkaStreamsProcessor implements ProcessorSession {
    private static final String DEDUPE_STORE = "metadata-dedup";
    private static final String AGGREGATE_STORE = "metadata-aggregates";
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
            builder.addStateStore(Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(DEDUPE_STORE), Serdes.String(), Serdes.String())
                    .withLoggingDisabled());
            builder.addStateStore(Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore(AGGREGATE_STORE), Serdes.String(), Serdes.String())
                    .withLoggingDisabled());
            input.process(() -> metadataProcessor(command), DEDUPE_STORE, AGGREGATE_STORE)
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

    private Processor<String, String, String, String> metadataProcessor(WorkerCommand command) {
        return new Processor<>() {
            private ProcessorContext<String, String> context;
            private KeyValueStore<String, String> dedupe;
            private KeyValueStore<String, String> aggregates;

            @Override public void init(ProcessorContext<String, String> context) {
                this.context = context;
                dedupe = context.getStateStore(DEDUPE_STORE);
                aggregates = context.getStateStore(AGGREGATE_STORE);
            }

            @Override public void process(Record<String, String> record) {
                long started = System.nanoTime();
                InputEvent input = EventCodec.readInput(record.value());
                if (!command.runId().equals(input.runId())) return;
                if (input.sequenceNumber() < 0) {
                    publishAggregates(record.timestamp());
                    return;
                }
                metrics.ingested();
                consumed.incrementAndGet();
                if (!input.eventId().equals(dedupe.get(input.key()))) {
                    dedupe.put(input.key(), input.eventId());
                    String aggregateKey = Workload.aggregateKey(record.timestamp(), input.key());
                    String previousValue = aggregates.get(aggregateKey);
                    OutputEvent previous = previousValue == null ? null : EventCodec.readOutput(previousValue);
                    aggregates.put(aggregateKey, EventCodec.write(
                            Workload.aggregate(input, previous, System.currentTimeMillis())));
                }
                metrics.processed(System.nanoTime() - started);
            }

            private void publishAggregates(long timestamp) {
                long started = System.nanoTime();
                ArrayList<String> publishedKeys = new ArrayList<>();
                try (KeyValueIterator<String, String> iterator = aggregates.all()) {
                    while (iterator.hasNext()) {
                        KeyValue<String, String> aggregate = iterator.next();
                        OutputEvent output = EventCodec.readOutput(aggregate.value);
                        context.forward(new Record<>(output.key(), aggregate.value, timestamp));
                        forwarded.incrementAndGet();
                        publishedKeys.add(aggregate.key);
                    }
                }
                publishedKeys.forEach(aggregates::delete);
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
