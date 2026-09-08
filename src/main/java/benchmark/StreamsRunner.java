package benchmark;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class StreamsRunner {
    BenchmarkResult run(Config config, int eventCount, int requestedPartitions, int actualPartitions,
                        int iteration, long seed) throws Exception {
        String runId = RunSupport.runId("kafka-streams");
        AtomicInteger consumed = new AtomicInteger();
        AtomicInteger forwarded = new AtomicInteger();
        String applicationId = "streams-" + runId;
        KafkaSupport.prepareGroupAtEnd(config, applicationId, config.inputTopic());
        long gcCount = ResourceSampler.gcCount();
        long gcTime = ResourceSampler.gcTime();
        Generation generation = RunSupport.generate(config, runId, eventCount, seed);
        StageMetrics metrics = new StageMetrics();

        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(config.inputTopic(), Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && value.contains("\"runId\":\"" + runId + "\""))
                .processValues(() -> new FixedKeyProcessor<String, String, String>() {
                    private FixedKeyProcessorContext<String, String> context;
                    @Override public void init(FixedKeyProcessorContext<String, String> context) { this.context = context; }
                    @Override public void process(FixedKeyRecord<String, String> record) {
                        metrics.ingested(Math.max(0, (System.currentTimeMillis() - record.timestamp()) * 1_000_000));
                        long started = System.nanoTime();
                        InputEvent input = EventCodec.readInput(record.value());
                        OutputEvent output = Workload.transform(input, System.currentTimeMillis());
                        String outputJson = EventCodec.write(output);
                        metrics.processed(System.nanoTime() - started);
                        consumed.incrementAndGet();
                        context.forward(record.withValue(outputJson));
                        forwarded.incrementAndGet();
                    }
                })
                .to(config.outputTopic(), Produced.with(Serdes.String(), Serdes.String()));

        Properties properties = config.kafkaProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.threads());
        properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, config.processingGuarantee());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, Path.of(System.getProperty("java.io.tmpdir"), runId).toString());
        properties.put("auto.offset.reset", "earliest");

        try (ResourceSampler sampler = new ResourceSampler();
             OutputCollector collector = new OutputCollector(config, runId, eventCount, metrics);
             KafkaStreams streams = new KafkaStreams(builder.build(), properties)) {
            CountDownLatch running = new CountDownLatch(1);
            streams.setStateListener((next, previous) -> {
                if (next == KafkaStreams.State.RUNNING) running.countDown();
            });
            streams.start();
            if (!running.await(config.timeoutSeconds(), TimeUnit.SECONDS)) {
                throw new IllegalStateException("Kafka Streams did not reach RUNNING state");
            }
            boolean completed = collector.await(Duration.ofSeconds(config.timeoutSeconds()));
            long finished = System.nanoTime();
            streams.close(Duration.ofSeconds(30));
            if (!completed) System.err.println("Kafka Streams run timed out: " + runId);
            Validation validation = collector.validation(generation.sent(), consumed.get(), forwarded.get());
            sampler.close();
            return RunSupport.result(config, "Kafka Streams", runId, iteration, eventCount, requestedPartitions,
                    actualPartitions, generation, metrics, finished, sampler.result(), validation, gcCount, gcTime);
        }
    }
}
