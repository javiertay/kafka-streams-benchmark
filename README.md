# Kafka Streams vs Plain Java Benchmark

A standalone Java 25 benchmark comparing exactly two equivalent Kafka pipelines:

```text
benchmark-input → Kafka Streams                    → benchmark-output
benchmark-input → KafkaConsumer + KafkaProducer    → benchmark-output
```

The benchmark asks: with the same Java runtime, Kafka cluster, deterministic JSON data, partitions, resource limits, processing logic, acknowledgements, and at-least-once delivery, which implementation is faster and which uses less CPU and RAM? It exists to replace assumptions about framework overhead with measurements from the target Kafka environment.

## What it measures

- **Ingestion throughput and p50/p95/p99 latency:** receiving records from Kafka.
- **Processing throughput and p50/p95/p99 latency:** JSON decoding, a deterministic hash-like transformation, output construction, and JSON encoding.
- **Publishing throughput and p50/p95/p99 latency:** time from processing completion until the output observer receives the record. This is publish-to-observe latency, not asynchronous API submission time. Kafka Streams does not expose per-record producer callbacks, so this definition is used for both implementations.
- **Total elapsed time and throughput:** processor start until all expected outputs are observed.
- **End-to-end p50/p95/p99 latency:** input generation through output observation.
- **Average/peak CPU and RAM:** samples for the single benchmark JVM during each implementation run.
- **Technical details:** Java/Kafka versions, GC, heap limit, JVM flags, GC activity, and safe client settings.

The static report explains these measurements for non-developers, highlights a winner only for valid runs, and treats values tied after rounding to two decimal places as ties.

## Requirements

- Docker with access to an existing Kafka cluster.
- For `SSL` or `SASL_SSL`, Kafka broker certificates trusted by a JKS or PKCS12 truststore.
- Permission to describe/create topics and increase partition counts.
- Enough disk space for two retained benchmark topics and the requested event volumes.

Kafka is external and is not included. The build and runtime use Eclipse Temurin Java 25. Maven runs inside the multi-stage Docker build; no host Maven installation is required.

## Local plaintext smoke test

[`compose.yaml`](compose.yaml) starts two local services:

- `kafka`: the official Apache Kafka 4.1.0 image running one combined KRaft broker/controller with plaintext listeners.
- `benchmark`: this repository's image, configured to connect to `kafka:19092` with `KAFKA_SECURITY_PROTOCOL=PLAINTEXT`.

The local defaults run a load matrix with:

- 100,000 measured events per run;
- requested input rates of 100,000 and 1,000,000 events/second;
- 1, 3, and 6 partitions;
- 1, 3, and 6 consumer instances; and
- 10,000 warm-up events followed by one measured iteration.

For Kafka Streams, a consumer instance means one stream thread created by `num.stream.threads`. For plain Java, it means one worker-owned `KafkaConsumer`. The report groups each rate × partition × consumer-count combination separately and shows the requested and actually achieved input rate. A local single broker may not achieve one million events/second; that shortfall is a result, not a test failure.

For fixed-rate scenarios, the processor and output observer start first. The workload producer then submits events at the requested pace while consumption and publishing run concurrently. This creates real input pressure rather than preloading the topic. `actual achieved input rate` includes the final producer flush and shows the rate the local generator and broker really sustained.

Start both services:

```bash
docker compose up --build
```

Wait for the benchmark log to print `Results available at http://localhost:8080`, then open <http://localhost:8080>. Kafka is also reachable from host tools at `localhost:9092`.

Stop and remove the local containers and network:

```bash
docker compose down
```

The Compose setup intentionally has no persistent volumes. Its topics and benchmark results disappear with the containers. The default matrix processes millions of records and can take several minutes. For a sustained one-million-events/second attempt, increase `BENCHMARK_EVENT_COUNTS` to `1000000`. This single-node plaintext broker is for local testing only and must not be used as a production Kafka configuration.

## Build and run

```bash
docker build -t kafka-streams-vs-java-benchmark .
```

```bash
docker run --rm \
  -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9093 \
  -e KAFKA_SECURITY_PROTOCOL=SSL \
  -e KAFKA_TRUSTSTORE_LOCATION=/certs/kafka.truststore.jks \
  -e KAFKA_TRUSTSTORE_PASSWORD=changeit \
  -e BENCHMARK_EVENT_COUNTS=10000,100000,1000000 \
  -e BENCHMARK_PARTITIONS=1,3,6,12 \
  -e BENCHMARK_PAYLOAD_BYTES=1024 \
  -e BENCHMARK_PROCESSING_THREADS=6 \
  -v /local/path/kafka.truststore.jks:/certs/kafka.truststore.jks:ro \
  -v ./benchmark-results:/app/results \
  kafka-streams-vs-java-benchmark
```

Open <http://localhost:8080> after the runs finish. The container stays alive to serve the report. Progress appears in the container logs.

The `/app/results` volume is optional. Omit `-v ./benchmark-results:/app/results` when results only need to remain available through the browser while the container is running. Keep the mount when the HTML and JSON files must survive container removal.

## Configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | Kafka bootstrap addresses |
| `KAFKA_SECURITY_PROTOCOL` | `SSL` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_TRUSTSTORE_LOCATION` | required for SSL protocols | Truststore path inside the container |
| `KAFKA_TRUSTSTORE_PASSWORD` | required for SSL protocols | Truststore password; never persisted |
| `KAFKA_PROPERTY_*` | none | Extra Kafka property: suffix is lowercased and `_` becomes `.` |
| `BENCHMARK_INPUT_TOPIC` | `benchmark-input` | Retained input topic |
| `BENCHMARK_OUTPUT_TOPIC` | `benchmark-output` | Retained output topic |
| `BENCHMARK_EVENT_COUNTS` | `10000,100000,1000000` | Comma-separated event volumes |
| `BENCHMARK_PARTITIONS` | `1,3,6,12` | Strictly ascending partition scenarios |
| `BENCHMARK_PAYLOAD_BYTES` | `1024` | Random source bytes before Base64 JSON encoding |
| `BENCHMARK_UNIQUE_KEYS` | `1000` | Number of repeatable keys |
| `BENCHMARK_PROCESSING_THREADS` | `1` | Comma-separated consumer-count scenarios: Kafka Streams stream threads / plain consumer workers |
| `BENCHMARK_WARMUP_EVENTS` | `1000` | Unreported warm-up events per implementation and scenario |
| `BENCHMARK_ITERATIONS` | `3` | Measured iterations; median is primary |
| `BENCHMARK_INPUT_RATES` | `0` | Comma-separated requested events/sec; `0` means unthrottled |
| `BENCHMARK_INPUT_RATE` | none | Single-rate form used when `BENCHMARK_INPUT_RATES` is absent |
| `BENCHMARK_REPLICATION_FACTOR` | `1` | Replication factor for newly created topics |
| `BENCHMARK_TIMEOUT_SECONDS` | `600` | Per-run completion timeout |
| `BENCHMARK_HTTP_PORT` | `8080` | Report server port |
| `BENCHMARK_RESULTS_DIR` | `/app/results` | Raw JSON, summary JSON, and HTML location |
| `KAFKA_STREAMS_PROCESSING_GUARANTEE` | `at_least_once` | Kafka Streams guarantee |

`KAFKA_SECURITY_PROTOCOL` is shared by the admin client, workload producer, output observer, Kafka Streams, and conventional clients. Truststore settings are validated only for `SSL` and `SASL_SSL`. For example, `KAFKA_PROPERTY_SSL_TRUSTSTORE_TYPE=PKCS12` supplies `ssl.truststore.type=PKCS12`. Extra properties can configure SASL, hostname verification, or mutual TLS, but secrets supplied this way should be treated as environment secrets. They are not copied into result files.

## How Kafka Streams consumes and publishes

The complete setup is in `KafkaSupport.streamsProperties` and `StreamsRunner.buildTopology`.

### Client configuration

| Setting | Value | Why |
|---|---|---|
| `application.id` | `streams-<unique runId>` | Gives every run its own consumer group and prevents committed offsets from another run being reused. |
| `bootstrap.servers` | `KAFKA_BOOTSTRAP_SERVERS` | Connects to the supplied external cluster. |
| `security.protocol` | `KAFKA_SECURITY_PROTOCOL` (`SSL` by default) | Selects the Kafka transport/authentication protocol for every client. |
| `ssl.truststore.location/password` | supplied environment values | Establishes broker trust; the password is not persisted. |
| default key/value Serdes | Kafka `StringSerde` | Both topics contain string keys and JSON string values. Jackson handles the JSON itself. |
| `num.stream.threads` | selected `BENCHMARK_PROCESSING_THREADS` value | Gives each Kafka Streams scenario the requested number of stream-thread consumer instances. |
| `processing.guarantee` | `at_least_once` by default | Matches the baseline delivery semantics. |
| `auto.offset.reset` | `earliest` fallback | Used only when a committed starting offset is unavailable. |
| `consumer.max.poll.records` | `1000` | Matches the conventional consumer batch limit. |
| `producer.acks` | `all` | Waits for all in-sync replicas before an output is acknowledged. |
| `producer.enable.idempotence` | `true` | Prevents producer retries from creating duplicate Kafka writes. |
| `state.dir` | a unique temporary run directory | Prevents local Kafka Streams state from being shared across benchmark runs. |

Before input is generated, `prepareGroupAtEnd` records the current end offset for the new Streams application ID. The generated records begin after that position. This avoids spending measured time scanning retained events from older benchmark runs.

### Topology and lifecycle

```text
benchmark-input
  → consume String key + JSON String value
  → keep only records containing this runId
  → deserialize with Jackson
  → execute Workload.transform
  → serialize the OutputEvent with Jackson
  → publish to benchmark-output
```

`StreamsRunner` waits until Kafka Streams reaches `RUNNING`, then waits for the independent output observer to see every expected output. It closes the Streams instance before finalizing the result. The `consumed` counter is incremented after successful processing; `forwarded` is incremented after the record is passed to the Kafka Streams sink. Kafka Streams owns polling, partition assignment, committing, and its internal producer.

## How the conventional consumer and publisher are set up

The complete setup is in `KafkaSupport.consumerProperties`, `KafkaSupport.producerProperties`, and `PlainRunner`.

### Consumer configuration

| Setting | Value | Why |
|---|---|---|
| `group.id` | `plain-<unique runId>` | Isolates every plain Java run. |
| key/value deserializers | Kafka `StringDeserializer` | Produces the same string key and JSON string consumed by Kafka Streams. |
| `auto.offset.reset` | `earliest` fallback | Used only if the prepared committed offset is unavailable. |
| `enable.auto.commit` | `false` | Prevents timer-driven commits from affecting a measured run. |
| `max.poll.records` | `1000` | Defines the maximum records returned by one poll. |
| SSL settings | same shared settings as Kafka Streams | Uses the same cluster and truststore configuration. |

One worker is created for the selected `BENCHMARK_PROCESSING_THREADS` scenario value. Each worker owns its own `KafkaConsumer`, because a consumer is not thread-safe. All consumers join the same run-specific group, so Kafka assigns each input partition to at most one worker. Consumers beyond the partition count remain idle, which makes partition-limited scaling visible. As with Kafka Streams, the group is positioned at the topic end before the run's input is generated, avoiding historical retained records.

### Publisher configuration

| Setting | Value | Why |
|---|---|---|
| key/value serializers | Kafka `StringSerializer` | Publishes the same string key and JSON representation as Kafka Streams. |
| `acks` | `all` | Uses the same acknowledgement strength as Kafka Streams. |
| `enable.idempotence` | `true` | Makes retries safe from duplicate Kafka writes. |
| SSL settings | same shared settings as Kafka Streams | Uses the same external cluster and truststore. |

The workers share one `KafkaProducer`; Kafka's producer is thread-safe. For each matching record a worker deserializes JSON, invokes the shared `Workload.transform`, serializes the result, and calls `send`. The callback increments `published` only when Kafka reports success. After observation finishes, the producer is flushed before the result is finalized.

### Shared output observer

Both implementations use the same observer. Before either processor starts, it joins a unique group, obtains all `benchmark-output` assignments, and seeks them to the current end. It then accepts only the current `runId`, detects missing/duplicate/unexpected sequence numbers, and records publish-to-observe and end-to-end latency. This keeps historical output from retained topics out of the measurement.

## Topics and partition scenarios

Only `benchmark-input` and `benchmark-output` are used. Missing topics are created and both topics are retained; the application never deletes them. Unique run IDs, consumer groups, and record filtering prevent older retained records from entering current results.

Kafka partition counts can increase but cannot decrease. Scenarios must therefore be ascending. If either retained topic already exceeds a requested count, both topics are aligned to the higher count and that lower scenario is recorded as skipped rather than falsely reported. Topic replication factor applies only at creation time.

## Fairness and result validity

Both paths use Java 25 in the same JVM/image, Jackson JSON, identical generated payloads and transformation, the same two topics and partition count, `acks=all`, at-least-once semantics, comparable thread counts, JVM flags, CPU/memory limits, and sequential execution. Warm-ups are excluded. Each measured iteration uses the same deterministic seed for both paths; only the run ID and timestamps differ.

A result is valid only when the expected number of unique outputs is observed and missing, duplicate, and unexpected counts are zero. Invalid comparisons receive no green winner highlight. Raw files include sent, consumed, submitted/forwarded, observed, missing, duplicate, and unexpected counts.

For repeated runs, the HTML selects the median iteration by total throughput and shows its metrics, plus the minimum and maximum total-throughput range. It never picks the fastest run as the headline result.

## Local Maven development

With Java 25 and Maven 3.9+ installed:

```bash
mvn verify
```

The test suite covers deterministic generation, JSON round trips, transformation equivalence, percentiles/medians, validation, configuration, retained-topic skip decisions, HTML generation, higher/lower winner rules, ties, and invalid comparisons. Kafka integration itself requires a real SSL cluster and is intentionally not mocked.

## Output files

- `raw-<runId>.json`: one machine-readable measured iteration.
- `summary.json`: all measured results and skipped scenarios.
- `index.html`: self-contained static comparison report.

Passwords are excluded. Mount `/app/results` to persist files after container removal.

## Known limitations

- Results are specific to the supplied cluster, network, container limits, topic state, and configuration; they are not universal performance claims.
- Publish latency is publish-to-observe rather than broker-ack latency so the two implementations use one comparable definition.
- Process RAM uses Linux resident set size in the container; the fallback outside Linux is used JVM heap.
- CPU and RAM are whole-process samples. Kafka Streams and plain Java run sequentially in that same process; the shared load generator runs concurrently and contributes to both measurements.
- Retained topics accumulate benchmark records and require an external retention policy appropriate for the test environment.
- The basic HTTP server has no authentication; expose it only on a trusted network.
