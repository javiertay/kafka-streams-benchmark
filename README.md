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
- **Offered versus achieved input rate:** whether the independent generator can deliver the requested load for the configured measurement window.
- **Backlog and catch-up time:** how many generated events remain unobserved when the measurement window closes, and how long processors need to drain them.
- **Average/peak CPU and RAM:** aggregate samples across processor service JVMs only; generator and observer resources are excluded.
- **Technical details:** Java/Kafka versions, GC, heap limit, JVM flags, GC activity, and safe client settings.

The static report starts with an overall verdict based on configuration wins, identifies the broker count and replication factor, has one tab per duration × rate × partition × service-count configuration, explains the measurements for non-developers, highlights a winner only for valid runs, and treats values tied after rounding to two decimal places as ties.

## Requirements

- Docker. The included Compose environment provides a local Kafka cluster; an existing cluster can be used instead.
- For `SSL` or `SASL_SSL`, Kafka broker certificates trusted by a JKS or PKCS12 truststore.
- Permission to describe/create topics and increase partition counts.
- Enough disk space for two retained benchmark topics and the requested event volumes.

The build and runtime use Eclipse Temurin Java 25. Maven runs inside the multi-stage Docker build; no host Maven installation is required.

## Local plaintext smoke test

[`compose.yaml`](compose.yaml) defines ten local services:

- `kafka-1` through `kafka-3`: the official Apache Kafka 4.1.0 image running up to three combined KRaft broker/controllers with plaintext listeners.
- `benchmark`: the coordinator, fixed-duration workload generator, output observer, result writer, and report server.
- `benchmark-worker-1` through `benchmark-worker-6`: separate JVM/container processor services activated by the coordinator as each scenario requires.

The cluster defaults to three active brokers and replication factor three. Set `KAFKA_BROKER_COUNT` to `1`, `2`, or `3`; broker containers above that number remain idle, and the benchmark topic and Kafka internal-topic replication factors follow the active broker count. Minimum in-sync replicas is one for the single-broker baseline and two for the two- or three-broker environments. The coordinator waits until Kafka reports exactly the configured number before starting the matrix, so a partially formed cluster fails clearly instead of producing a misleading result.

The local defaults run a load matrix with:

- a 10-second measured load window per run;
- requested input rates of 100,000 and 1,000,000 events/second;
- 1, 3, and 6 partitions;
- 1, 3, and 6 processor service instances; and
- a 1-second warm-up followed by one measured iteration.

Only service counts that can receive partitions are run. The default partition/service pairs are therefore `1/1`, `3/1`, `3/3`, `6/1`, `6/3`, and `6/6`, producing 12 configurations across the two input rates instead of the redundant 18-configuration cross-product. For Kafka Streams, each active worker starts one Kafka Streams instance with one stream thread and all workers share the same `application.id`. For plain Java, each active worker owns one `KafkaConsumer` and producer and all consumers share the same group ID. Kafka distributes partitions across separate services rather than threads in one JVM.

Processors and the output observer start first. The coordinator waits for Kafka to report the expected consumer-group membership, then produces for exactly `BENCHMARK_MEASUREMENT_SECONDS`. It does not force a preset event count. Achieved input rate is the number of records submitted during that window divided by its actual duration. The final producer flush is timed separately, and backlog is captured at the window boundary before that flush can hide consumer delay.

At the local 10-second default, the theoretical counts are one million events at 100k/s and ten million at 1m/s. If the generator reaches only 350k/s during the 1m/s scenario, the result reports roughly 3.5 million generated events and 350k/s rather than taking longer to force ten million records through. This distinguishes an unattained offered-load target from consumer performance.

Start the default three-broker environment:

```bash
docker compose up --build
```

To run the single-broker baseline instead, set the environment variable before starting Compose:

```bash
KAFKA_BROKER_COUNT=1 docker compose up --build
```

In PowerShell:

```powershell
$env:KAFKA_BROKER_COUNT = "1"
docker compose up --build
```

Wait for the benchmark log to print `[server] READY: benchmark complete; results available at http://localhost:8080`, then open <http://localhost:8080>. Host tools can use `localhost:9092`, `localhost:9093`, and `localhost:9094` for the active brokers.

### Reading benchmark progress

Coordinator logs use explicit lifecycle markers and scenario counters. A run is not finished merely because generation has stopped: the processors may still be draining a backlog. For example:

```text
[benchmark] Starting matrix: 12 scenarios, 2 implementations, 1 measured iteration, 10s measurement, 1s warm-up
[scenario 4/12] Starting: 10s, 3 partitions, 3 services, 100,000 events/s
[Kafka Streams][iteration 1] READY: all workers joined; generating for 10s at 100,000 events/s
[Kafka Streams][iteration 1] Generation window complete: 1,000,000 sent, 100,000 events/s achieved, 12,345 backlog, producer flush 0.08s
[Kafka Streams][iteration 1] Draining output: 987,655/1,000,000 observed (timeout 600s)
[Kafka Streams][iteration 1] Drain complete: 1,000,000/1,000,000 observed, catch-up 1.42s
[Kafka Streams][iteration 1] COMPLETED: consumed per service [333200, 333400, 333400], validation passed, elapsed 11.7s
[scenario 4/12] COMPLETED in 24.1s
```

`[benchmark] COMPLETED` means the whole matrix was processed. `[server] READY: benchmark complete` is the final success marker and means the completed report is available. A `[benchmark] FAILED` or per-run `TIMEOUT`/`INVALID` marker means the report should be inspected as a failed or invalid benchmark rather than a valid comparison. Worker-container logs separately show `STARTING`, `RUNNING`, `STOPPING`, and `STOPPED` for each assigned run. No per-record logs are emitted because they would distort the benchmark.

Stop and remove the local containers and network:

```bash
docker compose down
```

The Compose setup intentionally has no persistent volumes. Its topics and benchmark results disappear with the containers. The default matrix can process many millions of records and takes several minutes. Increase `BENCHMARK_MEASUREMENT_SECONDS` to 30–60 for decision-grade sustained tests after checking broker disk capacity. This plaintext combined broker/controller topology is for local testing only and must not be used as a production Kafka configuration.

Run the one-broker and three-broker environments separately. Each invocation creates one report and one verdict, and the HTML names that report's broker count and replication factor. Do not merge their configuration wins: changing broker count and replication changes the environment, so compare the two reports side by side.

## Build and run

```bash
docker build -t kafka-streams-vs-java-benchmark .
```

```bash
docker run --rm \
  -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9093 \
  -e KAFKA_BROKER_COUNT=3 \
  -e KAFKA_SECURITY_PROTOCOL=SSL \
  -e KAFKA_TRUSTSTORE_LOCATION=/certs/kafka.truststore.jks \
  -e KAFKA_TRUSTSTORE_PASSWORD=changeit \
  -e BENCHMARK_INPUT_RATES=100000,1000000 \
  -e BENCHMARK_MEASUREMENT_SECONDS=30 \
  -e BENCHMARK_PARTITIONS=1,3,6,12 \
  -e BENCHMARK_PAYLOAD_BYTES=1024 \
  -e BENCHMARK_SERVICE_INSTANCES=1 \
  -e BENCHMARK_WORKER_URLS=http://benchmark-worker-1:8080 \
  -e BENCHMARK_REPLICATION_FACTOR=3 \
  -v /local/path/kafka.truststore.jks:/certs/kafka.truststore.jks:ro \
  -v ./benchmark-results:/app/results \
  kafka-streams-vs-java-benchmark
```

That coordinator example assumes a separately started container named `benchmark-worker-1` on the same Docker network, using the same Kafka/security variables plus `BENCHMARK_WORKER_MODE=true`. Use `compose.yaml` as the complete runnable reference; worker ports stay private.

Open <http://localhost:8080> after the runs finish. The container stays alive to serve the report. Progress appears in the container logs.

The `/app/results` volume is optional. Omit `-v ./benchmark-results:/app/results` when results only need to remain available through the browser while the container is running. Keep the mount when the HTML and JSON files must survive container removal.

## Configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | Kafka bootstrap addresses |
| `KAFKA_BROKER_COUNT` | `1` | Exact broker count expected in the connected cluster; Compose accepts `1`, `2`, or `3` and defaults it to `3` |
| `KAFKA_SECURITY_PROTOCOL` | `SSL` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` |
| `KAFKA_TRUSTSTORE_LOCATION` | required for SSL protocols | Truststore path inside the container |
| `KAFKA_TRUSTSTORE_PASSWORD` | required for SSL protocols | Truststore password; never persisted |
| `KAFKA_PROPERTY_*` | none | Extra Kafka property: suffix is lowercased and `_` becomes `.` |
| `BENCHMARK_INPUT_TOPIC` | `benchmark-input` | Retained input topic |
| `BENCHMARK_OUTPUT_TOPIC` | `benchmark-output` | Retained output topic |
| `BENCHMARK_PARTITIONS` | `1,3,6,12` | Strictly ascending partition scenarios |
| `BENCHMARK_PAYLOAD_BYTES` | `1024` | Random source bytes before Base64 JSON encoding |
| `BENCHMARK_UNIQUE_KEYS` | `1000` | Number of repeatable keys |
| `BENCHMARK_SERVICE_INSTANCES` | `1` | Comma-separated processor service/container counts; values greater than a scenario's requested partitions are omitted |
| `BENCHMARK_WORKER_URLS` | none | Comma-separated worker base URLs; at least the largest requested service count is required |
| `BENCHMARK_WORKER_MODE` | `false` | Runs this image as a coordinator-controlled processor worker |
| `BENCHMARK_WARMUP_SECONDS` | `2` | Unreported fixed-duration warm-up per implementation and scenario; `0` disables it |
| `BENCHMARK_MEASUREMENT_SECONDS` | `30` | Fixed offered-load window used to derive the maximum possible event count |
| `BENCHMARK_ITERATIONS` | `3` | Measured iterations; median is primary |
| `BENCHMARK_INPUT_RATES` | `100000,1000000` | Comma-separated positive offered-load targets in events/sec |
| `BENCHMARK_REPLICATION_FACTOR` | `KAFKA_BROKER_COUNT` | Replication factor for newly created topics; cannot exceed the configured broker count |
| `BENCHMARK_TIMEOUT_SECONDS` | `600` | Per-run completion timeout |
| `BENCHMARK_HTTP_PORT` | `8080` | Report server port |
| `BENCHMARK_RESULTS_DIR` | `/app/results` | Raw JSON, summary JSON, and HTML location |
| `KAFKA_STREAMS_PROCESSING_GUARANTEE` | `at_least_once` | Kafka Streams guarantee |

`KAFKA_BROKER_COUNT` describes one benchmark environment; it is not another scenario axis inside the report. The coordinator checks the actual cluster membership before creating topics. Use a separate results directory or preserve each generated report before changing this value.

`KAFKA_SECURITY_PROTOCOL` is shared by the admin client, workload producer, output observer, Kafka Streams, and conventional clients. Truststore settings are validated only for `SSL` and `SASL_SSL`. For example, `KAFKA_PROPERTY_SSL_TRUSTSTORE_TYPE=PKCS12` supplies `ssl.truststore.type=PKCS12`. Extra properties can configure SASL, hostname verification, or mutual TLS, but secrets supplied this way should be treated as environment secrets. They are not copied into result files.

## How Kafka Streams consumes and publishes

The complete implementation is in `KafkaStreamsProcessor`. Its `buildTopology` method shows the Kafka Streams DSL from input topic to output topic, `processor` contains the per-record work, and `stop` contains shutdown and metric reporting. Shared Kafka client settings remain in `KafkaSupport.streamsProperties`.

### Client configuration

| Setting | Value | Why |
|---|---|---|
| `application.id` | `streams-<unique runId>` shared by active workers | Gives every run its own distributed consumer group and prevents committed offsets from another run being reused. |
| `bootstrap.servers` | `KAFKA_BOOTSTRAP_SERVERS` | Connects to the supplied external cluster. |
| `security.protocol` | `KAFKA_SECURITY_PROTOCOL` (`SSL` by default) | Selects the Kafka transport/authentication protocol for every client. |
| `ssl.truststore.location/password` | supplied environment values | Establishes broker trust; the password is not persisted. |
| default key/value Serdes | Kafka `StringSerde` | Both topics contain string keys and JSON string values. Jackson handles the JSON itself. |
| `num.stream.threads` | `1` per worker service | Measures horizontal service scaling rather than extra threads inside one JVM. |
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

Every active worker waits until Kafka Streams reaches `RUNNING`. The coordinator then waits for all requested members to join the application group before producing. After the timed generation window, it waits for the independent output observer to see every generated event, stops all workers, and aggregates their counters, latency samples, CPU, RAM, and GC measurements.

## How the conventional consumer and publisher are set up

The complete implementation is in `TraditionalKafkaProcessor`. Its `consume` method is the conventional poll loop, `process` contains the per-record decode/transform/send sequence, and `stop` shows consumer wake-up and producer flushing. Shared client settings remain in `KafkaSupport.consumerProperties` and `KafkaSupport.producerProperties`.

### Consumer configuration

| Setting | Value | Why |
|---|---|---|
| `group.id` | `plain-<unique runId>` shared by active workers | Isolates every run while Kafka distributes partitions across processor services. |
| key/value deserializers | Kafka `StringDeserializer` | Produces the same string key and JSON string consumed by Kafka Streams. |
| `auto.offset.reset` | `earliest` fallback | Used only if the prepared committed offset is unavailable. |
| `enable.auto.commit` | `false` | Prevents timer-driven commits from affecting a measured run. |
| `max.poll.records` | `1000` | Defines the maximum records returned by one poll. |
| SSL settings | same shared settings as Kafka Streams | Uses the same cluster and truststore configuration. |

Each active service owns one `KafkaConsumer`, because a consumer is not thread-safe. All consumers join the same run-specific group, so Kafka assigns each input partition to at most one service. Configurations requesting more services than partitions are omitted because the extra services would necessarily be idle. The group is positioned at the topic end before workers start and before input is generated, avoiding historical retained records.

### Publisher configuration

| Setting | Value | Why |
|---|---|---|
| key/value serializers | Kafka `StringSerializer` | Publishes the same string key and JSON representation as Kafka Streams. |
| `acks` | `all` | Uses the same acknowledgement strength as Kafka Streams. |
| `enable.idempotence` | `true` | Makes retries safe from duplicate Kafka writes. |
| SSL settings | same shared settings as Kafka Streams | Uses the same external cluster and truststore. |

Each worker owns one `KafkaProducer`. For each matching record it deserializes JSON, invokes the shared `Workload.transform`, serializes the result, and calls `send`. The callback increments `published` only when Kafka reports success. Every producer is flushed before the result is finalized.

### Shared output observer

Both implementations use the same observer. Before either processor starts, it joins a unique group, obtains all `benchmark-output` assignments, and seeks them to the current end. It then accepts only the current `runId`, detects missing/duplicate/unexpected sequence numbers, and records publish-to-observe and end-to-end latency. This keeps historical output from retained topics out of the measurement.

## Topics and partition scenarios

Only `benchmark-input` and `benchmark-output` are used. Missing topics are created and both topics are retained; the application never deletes them. Unique run IDs, consumer groups, and record filtering prevent older retained records from entering current results.

Kafka partition counts can increase but cannot decrease. Scenarios must therefore be ascending. If either retained topic already exceeds a requested count, both topics are aligned to the higher count and that lower scenario is recorded as skipped rather than falsely reported. Topic replication factor applies only at creation time.

## Fairness and result validity

Both paths use Java 25, the same image and service count, Jackson JSON, identical generation duration/rate targets, payloads and transformation, the same two topics and partition count, `acks=all`, at-least-once semantics, JVM flags, CPU/memory limits, and sequential execution. Warm-ups are excluded. Each measured iteration uses the same deterministic seed for both paths; only the run ID and timestamps differ.

A result is valid only when the expected number of unique outputs is observed and missing, duplicate, and unexpected counts are zero. Invalid comparisons receive no green winner highlight. Raw files include sent, consumed, submitted/forwarded, observed, missing, duplicate, and unexpected counts.

For repeated runs, the HTML selects the median iteration by total throughput and shows its metrics, plus the minimum and maximum total-throughput range. It never picks the fastest run as the headline result. Latency queues retain at most 100,000 evenly spaced samples per stage so a sustained 1m/s test does not exhaust the JVM heap.

The overall summary gives each complete, valid configuration one vote according to which implementation has higher median total throughput. It reports Kafka Streams wins, plain Java wins, ties, and excluded invalid comparisons. It does not average throughput across different offered rates, partition counts, or service counts.

## Interpreting sustained-load results

A processor sustained the requested load only when all three observations agree:

- achieved input rate is close to the requested rate;
- backlog at generation end is near zero; and
- catch-up time is near zero.

If achieved input rate is far below the target, the generator, broker, or network was the bottleneck and the run did not actually test consumers at that target. If achieved rate is close but backlog grows, the processors could not keep up. If backlog is near zero, compare end-to-end latency and aggregate processor CPU/RAM to choose between implementations.

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
- CPU and RAM are summed across processor worker services; the separate coordinator's generator, observer, report server, and orchestration overhead are excluded.
- Latency percentiles are based on at most 100,000 evenly spaced samples per stage for bounded memory use.
- Worker control uses unauthenticated HTTP on the private container network; do not publish worker ports.
- Retained topics accumulate benchmark records and require an external retention policy appropriate for the test environment.
- The basic HTTP server has no authentication; expose it only on a trusted network.
