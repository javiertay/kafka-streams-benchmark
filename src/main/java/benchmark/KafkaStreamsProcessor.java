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

import java.time.Duration;
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
        builder.stream(config.inputTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && value.contains("\"runId\":\"" + command.runId() + "\""))
                .processValues(this::processor)
                .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private FixedKeyProcessor<String, String, String> processor() {
        return new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<String, String> context;

            @Override public void init(FixedKeyProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override public void process(FixedKeyRecord<String, String> record) {
                metrics.ingested(Math.max(0, (System.currentTimeMillis() - record.timestamp()) * 1_000_000));
                long started = System.nanoTime();
                OutputEvent output = Workload.transform(
                        EventCodec.readInput(record.value()), System.currentTimeMillis());
                metrics.processed(System.nanoTime() - started);
                consumed.incrementAndGet();
                context.forward(record.withValue(EventCodec.write(output)));
                forwarded.incrementAndGet();
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
        return new ProcessorReport(consumed.get(), forwarded.get(), metrics.snapshot(), resources.result(),
                Math.max(0, ResourceSampler.gcCount() - initialGcCount),
                Math.max(0, ResourceSampler.gcTime() - initialGcTime));
    }
}
