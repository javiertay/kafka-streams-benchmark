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

final class StreamsRunner {
    BenchmarkResult run(Config config, int eventCount, int requestedPartitions, int actualPartitions,
                        int processingThreads, long inputRate, int iteration, long seed) throws Exception {
        String runId = RunSupport.runId("kafka-streams");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger forwarded = new AtomicInteger();
        String applicationId = "streams-" + runId;
        KafkaSupport.prepareGroupAtEnd(config, applicationId, config.inputTopic());
        long gcCount = ResourceSampler.gcCount();
        long gcTime = ResourceSampler.gcTime();
        StageMetrics metrics = new StageMetrics();
        Topology topology = buildTopology(config, runId, metrics, consumed, forwarded);

        try (ResourceSampler sampler = new ResourceSampler();
             OutputCollector collector = new OutputCollector(config, runId, eventCount, metrics);
             KafkaStreams streams = new KafkaStreams(topology,
                     KafkaSupport.streamsProperties(config, applicationId, runId, processingThreads))) {
            CountDownLatch running = new CountDownLatch(1);
            streams.setStateListener((next, previous) -> {
                if (next == KafkaStreams.State.RUNNING) running.countDown();
            });
            streams.start();
            if (!running.await(config.timeoutSeconds(), TimeUnit.SECONDS)) {
                throw new IllegalStateException("Kafka Streams did not reach RUNNING state");
            }
            Generation generation = RunSupport.generate(config, runId, eventCount, inputRate, seed);
            boolean completed = collector.await(Duration.ofSeconds(config.timeoutSeconds()));
            long finished = System.nanoTime();
            streams.close(Duration.ofSeconds(30));
            if (!completed) System.err.println("Kafka Streams run timed out: " + runId);
            Validation validation = collector.validation(generation.sent(), consumed.get(), forwarded.get());
            sampler.close();
            return RunSupport.result(config, "Kafka Streams", runId, iteration, eventCount, requestedPartitions,
                    actualPartitions, processingThreads, inputRate, generation, metrics, finished,
                    sampler.result(), validation, gcCount, gcTime);
        }
    }

    private static Topology buildTopology(Config config, String runId, StageMetrics metrics,
                                          AtomicInteger consumed, AtomicInteger forwarded) {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(config.inputTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> belongsToRun(value, runId))
                .processValues(() -> processingStage(metrics, consumed, forwarded))
                .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    private static boolean belongsToRun(String json, String runId) {
        return json != null && json.contains("\"runId\":\"" + runId + "\"");
    }

    private static FixedKeyProcessor<String, String, String> processingStage(
            StageMetrics metrics, AtomicInteger consumed, AtomicInteger forwarded) {
        return new FixedKeyProcessor<>() {
            private FixedKeyProcessorContext<String, String> context;

            @Override public void init(FixedKeyProcessorContext<String, String> context) {
                this.context = context;
            }

            @Override public void process(FixedKeyRecord<String, String> record) {
                metrics.ingested(Math.max(0,
                        (System.currentTimeMillis() - record.timestamp()) * 1_000_000));
                long started = System.nanoTime();
                InputEvent input = EventCodec.readInput(record.value());
                OutputEvent output = Workload.transform(input, System.currentTimeMillis());
                String outputJson = EventCodec.write(output);
                metrics.processed(System.nanoTime() - started);
                consumed.incrementAndGet();
                context.forward(record.withValue(outputJson));
                forwarded.incrementAndGet();
            }
        };
    }
}
