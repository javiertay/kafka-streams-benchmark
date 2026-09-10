# Kafka Streams vs Plain Java Benchmark

A standalone Java 25 benchmark comparing exactly two equivalent Kafka pipelines:

```text
benchmark-input → Kafka Streams                    → benchmark-output
benchmark-input → KafkaConsumer + KafkaProducer    → benchmark-output
```

The benchmark asks: for the same deterministic variable-rate input schedule, Java runtime, Kafka cluster, JSON data, partitions, resource limits, processing logic, acknowledgements, and at-least-once delivery, which implementation performs better and which uses less CPU and RAM? It exists to replace assumptions about framework overhead with measurements from the target Kafka environment.

## What it measures

- **Ingestion elapsed time and throughput:** receiving the complete live input schedule from Kafka.
- **Stage elapsed time and throughput:** ingestion, processing, and publishing wall times are shown directly. Per-record processing p50/p95/p99 is identically timed from JSON decoding through business logic and output handoff for both implementations.
- **Metadata flush p50/p95/p99 latency:** partition-local consolidation and global payload merge at each configured output interval.
- **Publishing throughput and p50/p95/p99 latency:** time from processing completion until the output observer receives the record. This is publish-to-observe latency, not asynchronous API submission time. Kafka Streams does not expose per-record producer callbacks, so this definition is used for both implementations.
- **Total elapsed time and input throughput:** processor worker launch until all expected outputs are observed. Throughput uses consumed inputs, not output count, so suppressed metadata output does not artificially lower processing capacity.
- **Average/peak CPU and RAM:** time-aligned aggregate samples across processor service JVMs only; generator and observer resources are excluded. Peaks are simultaneous totals, not sums of independently occurring per-service peaks.
- **Technical details:** Java/Kafka versions, GC, heap limit, JVM flags, GC activity, and safe client settings.

The static report starts with an overall verdict based on configuration wins, identifies the broker count and replication factor, has one tab per event-count × partition × service-count configuration, explains the measurements for non-developers, highlights a winner only for valid runs, and treats values tied after rounding to two decimal places as ties.

## Requirements

- Docker. The included Compose environment provides a local Kafka cluster; an existing cluster can be used instead.
- For `SSL` or `SASL_SSL`, Kafka broker certificates trusted by a JKS or PKCS12 truststore.
- Permission to describe/create topics and increase partition counts.
- Enough disk space for two retained benchmark topics and the requested event volumes.

The build and runtime use Eclipse Temurin Java 25. Maven runs inside the multi-stage Docker build; no host Maven installation is required.

## Local plaintext smoke test

[`compose.yaml`](compose.yaml) defines ten local services:

- `kafka-1` through `kafka-3`: the official Apache Kafka 4.1.2 image running up to three combined KRaft broker/controllers with plaintext listeners.
- `benchmark`: the coordinator, fixed-count workload generator, output observer, result writer, and report server.
- `benchmark-worker-1` through `benchmark-worker-6`: separate JVM/container processor services activated by the coordinator as each scenario requires.

The cluster defaults to three active brokers and replication factor three. Set `KAFKA_BROKER_COUNT` to `1`, `2`, or `3`; broker containers above that number remain idle, and the benchmark topic and Kafka internal-topic replication factors follow the active broker count. Minimum in-sync replicas is one for the single-broker baseline and two for the two- or three-broker environments. The coordinator waits until Kafka reports exactly the configured number before starting the matrix, so a partially formed cluster fails clearly instead of producing a misleading result.

The local defaults run a load matrix with:

- a deterministic input rate that changes every second between 10 and 500 records/second;
- a five-minute measured run and one consolidated metadata output every five seconds;
- 20% seeded duplicate events;
- the `metadata` processing mode, which deduplicates, aggregates, and suppresses intermediate output;
- 1, 3, and 6 partitions;
- 1, 3, and 6 processor service instances; and
- a 10-second warm-up followed by two measured iterations with alternating implementation order.

Only service counts that can receive partitions are run. The default partition/service pairs are therefore `1/1`, `3/1`, `3/3`, `6/1`, `6/3`, and `6/6`, producing six configurations. For Kafka Streams, each active worker starts one Kafka Streams instance with one stream thread and all workers share the same `application.id`. For plain Java, each active worker owns one `KafkaConsumer` and producer and all consumers share the same group ID. Kafka distributes partitions across separate services rather than threads in one JVM.

Before each paired iteration, the coordinator computes the complete per-second rate schedule from the fixed seed. After workers are ready, it streams that schedule in real time. Kafka Streams and Plain Java receive the same rate each second, the same deterministic event identities and payloads, and duplicates in the same positions. Metadata interval markers cause each partition to emit a partial consolidation; a second processing stage merges all partition partials into exactly one output payload per interval.

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

Coordinator logs use explicit lifecycle markers and scenario counters. For example:

```text
[benchmark] Processing mode: metadata
[benchmark] Starting matrix: 6 scenarios, 2 implementations, 2 measured iterations, 300s/run, 10s warm-up, 10-500 inputs/s
[scenario 4/6] Starting: 76,412 events, 6 partitions, 1 service
[Kafka Streams][iteration 1] STARTING: 1 service, 6 partitions, 76,412 scheduled input events over 300s
[Kafka Streams][iteration 1] MEASUREMENT START: streaming variable input for 300s
[Kafka Streams][iteration 1] Input complete: 76,412 records; draining 60 expected outputs
[Kafka Streams][iteration 1] Processing complete: 60/60 outputs observed
[scenario 4/6] COMPLETED in 24.1s
```

`[benchmark] COMPLETED` means the whole matrix was processed. `[server] READY: benchmark complete` is the final success marker and means the completed report is available. A `[benchmark] FAILED` or per-run `TIMEOUT`/`INVALID` marker means the report should be inspected as a failed or invalid benchmark rather than a valid comparison. Worker-container logs separately show `STARTING`, `RUNNING`, `STOPPING`, and `STOPPED` for each assigned run. No per-record logs are emitted because they would distort the benchmark.

Stop and remove the local containers and network:

```bash
docker compose down
```

The Compose setup intentionally has no persistent volumes. Its topics and benchmark results disappear with the containers. Increase the duration or per-second input range for a longer or heavier test after checking broker disk capacity. This plaintext combined broker/controller topology is for local testing only and must not be used as a production Kafka configuration.

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
  -e BENCHMARK_DURATION_SECONDS=300 \
  -e BENCHMARK_OUTPUT_INTERVAL_SECONDS=5 \
  -e BENCHMARK_MIN_INPUTS_PER_SECOND=10 \
  -e BENCHMARK_MAX_INPUTS_PER_SECOND=500 \
  -e BENCHMARK_DUPLICATE_PERCENT=20 \
  -e BENCHMARK_WORKLOAD_SEED=42 \
  -e BENCHMARK_PARTITIONS=1,3,6,12 \
  -e BENCHMARK_PAYLOAD_BYTES=1024 \
  -e BENCHMARK_PROCESSING_MODE=metadata \
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
| `BENCHMARK_PARTIAL_TOPIC` | `benchmark-metadata-partials` | Internal partition-local metadata consolidation topic |
| `BENCHMARK_OUTPUT_TOPIC` | `benchmark-output` | Retained output topic |
| `BENCHMARK_PARTITIONS` | `1,3,6,12` | Strictly ascending partition scenarios |
| `BENCHMARK_PAYLOAD_BYTES` | `1024` | Random source bytes before Base64 JSON encoding |
| `BENCHMARK_PROCESSING_MODE` | `transform` | `transform` for one input/one output, or `metadata` for periodic deduplication and consolidation; Compose defaults to `metadata` |
| `BENCHMARK_UNIQUE_KEYS` | `1000` | Number of repeatable keys |
| `BENCHMARK_SERVICE_INSTANCES` | `1` | Comma-separated processor service/container counts; values greater than a scenario's requested partitions are omitted |
| `BENCHMARK_WORKER_URLS` | none | Comma-separated worker base URLs; at least the largest requested service count is required |
| `BENCHMARK_WORKER_MODE` | `false` | Runs this image as a coordinator-controlled processor worker |
| `BENCHMARK_DURATION_SECONDS` | `300` | Number of seconds that variable-rate input is streamed per measured run |
| `BENCHMARK_OUTPUT_INTERVAL_SECONDS` | `5` | Seconds represented by each consolidated metadata output payload |
| `BENCHMARK_MIN_INPUTS_PER_SECOND` | `10` | Inclusive minimum input count selected for each second |
| `BENCHMARK_MAX_INPUTS_PER_SECOND` | `500` | Inclusive maximum input count selected for each second |
| `BENCHMARK_DUPLICATE_PERCENT` | `20` | Approximate deterministic percentage of metadata inputs that repeat the preceding event identity |
| `BENCHMARK_WORKLOAD_SEED` | `42` | Seed that makes the rate schedule, inputs, and duplicate positions repeatable |
| `BENCHMARK_WARMUP_DURATION_SECONDS` | `10` | Unreported warm-up duration per implementation and scenario; `0` disables it |
| `BENCHMARK_ITERATIONS` | `4` | Measured iterations; Compose uses `2`; must be an even number of at least two so execution order is balanced; median is primary |
| `BENCHMARK_REPLICATION_FACTOR` | `KAFKA_BROKER_COUNT` | Replication factor for newly created topics; cannot exceed the configured broker count |
| `BENCHMARK_TIMEOUT_SECONDS` | `600` | Per-run completion timeout |
| `BENCHMARK_HTTP_PORT` | `8080` | Report server port |
| `BENCHMARK_RESULTS_DIR` | `/app/results` | Raw JSON, summary JSON, and HTML location |
| `KAFKA_STREAMS_PROCESSING_GUARANTEE` | `at_least_once` | Kafka Streams guarantee |

`KAFKA_BROKER_COUNT` describes one benchmark environment; it is not another scenario axis inside the report. The coordinator checks the actual cluster membership before creating topics. Use a separate results directory or preserve each generated report before changing this value.

`KAFKA_SECURITY_PROTOCOL` is shared by the admin client, workload producer, output observer, Kafka Streams, and conventional clients. Truststore settings are validated only for `SSL` and `SASL_SSL`. For example, `KAFKA_PROPERTY_SSL_TRUSTSTORE_TYPE=PKCS12` supplies `ssl.truststore.type=PKCS12`. Extra properties can configure SASL, hostname verification, or mutual TLS, but secrets supplied this way should be treated as environment secrets. They are not copied into result files.

## Processing modes

`BENCHMARK_PROCESSING_MODE=transform` preserves the original framework-overhead baseline: every generated input is deserialized, transformed, and published as exactly one output.

`BENCHMARK_PROCESSING_MODE=metadata` models a stateful metadata processor:

1. A seeded schedule selects a different input count for every second within the configured minimum and maximum.
2. Seeded duplicate positions repeat a preceding event ID, track ID, and payload; later new events continue through the full track-ID range.
3. Each input partition deduplicates event IDs and builds a partial payload for the configured output interval.
4. Partition partials are sent through the internal metadata-partials topic and merged globally.
5. Exactly one final payload is emitted per interval, including sorted track IDs, total input count, unique-event count, and duplicate count.

The expected output count is `ceil(BENCHMARK_DURATION_SECONDS / BENCHMARK_OUTPUT_INTERVAL_SECONDS)`. A five-minute run with a five-second interval therefore expects 60 outputs. Payload hashes and all validation counters are calculated from the generated schedule, and a result is valid only when both implementations consume every scheduled input and produce every expected consolidated payload exactly once.

Both implementations deliberately use bounded, in-memory, per-run interval state for this performance comparison. This compares equivalent business processing without charging only one side for durable state recovery. It is not a failover or recovery benchmark.

Compose applies the selected mode to the coordinator and every worker. It defaults to metadata mode; set `BENCHMARK_PROCESSING_MODE=transform` in the shell before `docker compose up --build` to rerun the original baseline.

## How Kafka Streams consumes and publishes

The complete implementation is in `KafkaStreamsProcessor`. Its `buildTopology` method selects the transform or metadata topology, `transformProcessor` contains the stateless path, and `metadataProcessor` contains task-local deduplication, aggregation, and final publication. Shared Kafka client settings remain in `KafkaSupport.streamsProperties`.

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
| `commit.interval.ms` | `1000` | Matches the explicit one-second commit cadence in the conventional consumer. |
| `auto.offset.reset` | `earliest` fallback | Used only when a committed starting offset is unavailable. |
| `consumer.max.poll.records` | `1000` | Matches the conventional consumer batch limit. |
| `producer.acks` | `all` | Waits for all in-sync replicas before an output is acknowledged. |
| `producer.enable.idempotence` | `true` | Prevents producer retries from creating duplicate Kafka writes. |
| `state.dir` | a unique temporary run directory | Prevents local Kafka Streams state from being shared across benchmark runs. |

Before workers start, `prepareGroupAtEnd` records the current end offsets for the new Streams application ID on both the input and metadata-partials topics. Live input begins only after group membership is stable, preventing retained events from older runs from entering the measurement.

### Topology and lifecycle

```text
benchmark-input
  → consume String key + JSON String value
  → keep only records containing this runId
  → transform mode: deserialize → Workload.transform → one output per input
  → metadata mode: deduplicate event IDs and build a partition-local interval payload
  → publish the partial to benchmark-metadata-partials
  → globally merge all partition partials for the interval
  → publish one consolidated JSON payload to benchmark-output
```

Every active worker reaches `RUNNING` before the live schedule begins. At each output boundary, partition markers release local partial payloads to the internal topic; its single partition provides one global merge order. The coordinator waits for the independent output observer to see every expected final output, then stops all workers and aggregates their counters, latency samples, CPU, RAM, and GC measurements.

## How the conventional consumer and publisher are set up

The complete implementation is in `TraditionalKafkaProcessor`. Its `consume` method is the input poll loop, `process` contains the selected transform or metadata logic, `publishPartial` releases partition interval payloads, and `consumePartials` performs the global merge. Shared client settings remain in `KafkaSupport.consumerProperties` and `KafkaSupport.producerProperties`.

### Consumer configuration

| Setting | Value | Why |
|---|---|---|
| `group.id` | `plain-<unique runId>` shared by active workers | Isolates every run while Kafka distributes partitions across processor services. |
| key/value deserializers | Kafka `StringDeserializer` | Produces the same string key and JSON string consumed by Kafka Streams. |
| `auto.offset.reset` | `earliest` fallback | Used only if the prepared committed offset is unavailable. |
| `enable.auto.commit` | `false` | The processor flushes output and explicitly commits every second, matching Kafka Streams' at-least-once commit cadence and ordering. |
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

Each worker owns one `KafkaProducer`. Transform mode invokes `Workload.transform` and sends each result. In metadata mode the same consumer group subscribes to both input and partial topics: input assignments publish partition partials, while the worker assigned the partial topic merges them into one final payload. The callback increments `published` only when Kafka reports success. Every producer is flushed before the result is finalized.

### Shared output observer

Both implementations use the same observer. Before either processor starts, it joins a unique group, obtains all `benchmark-output` assignments, and seeks them to the current end. It then accepts only the current `runId`, detects missing/duplicate/unexpected sequence numbers, and records publish-to-observe latency. This keeps historical output from retained topics out of the measurement.

## Topics and partition scenarios

The benchmark uses `benchmark-input`, `benchmark-metadata-partials`, and `benchmark-output`. The partial topic has one partition so every interval's partition partials reach one global merger. Missing topics are created and retained; unique run IDs, consumer groups, and record filtering prevent older retained records from entering current results.

Kafka partition counts can increase but cannot decrease. Scenarios must therefore be ascending. If either retained topic already exceeds a requested count, both topics are aligned to the higher count and that lower scenario is recorded as skipped rather than falsely reported. Topic replication factor applies only at creation time.

## Fairness and result validity

Both paths use Java 25, the same seeded per-second input schedule, image and service count, Jackson JSON, payloads, duplicate positions and processing mode, the same topics and partition count, `acks=all`, one-second offset commits, at-least-once transport settings, JVM flags, CPU/memory limits, and sequential execution. Warm-ups are excluded. Only the run ID and wall-clock timestamps differ. Measured execution order alternates by iteration, and the required even iteration count gives each implementation the first and second position equally often.

Metadata mode deliberately performs the same aggregate-state JSON decode and encode in both implementations. Kafka Streams stores those strings in task-local in-memory stores while plain Java stores them in maps, leaving only the state-container/framework implementation as the measured difference.

A result is valid only when all scheduled inputs were sent and consumed, every expected output was published and observed, and missing, duplicate, unexpected, and incorrect-payload counts are zero. Invalid comparisons receive no green winner highlight. Raw files include the actual scheduled input total and all validation counters.

For repeated runs, the HTML selects the median iteration by total processing throughput and shows its metrics, plus the minimum and maximum throughput range. It never picks the fastest run as the headline result. Latency queues retain at most 100,000 evenly spaced samples per stage so a sustained high-volume test does not exhaust the JVM heap.

The overall summary gives each complete, valid configuration one vote according to which implementation has higher median total processing throughput. It reports Kafka Streams wins, plain Java wins, ties, and excluded invalid comparisons. It does not average throughput across different partition or service counts.

## Interpreting fixed-workload results

A faster implementation should show a consistent advantage across these observations:

- lower total processing elapsed time;
- higher total processing throughput; and
- acceptable latency and processor CPU/RAM.

Every paired run uses exactly the same seeded rate schedule and therefore the same total record count. Compare processing latency and resource use while the schedule is active, plus the small drain beyond the configured duration needed to observe the final interval payload.

## Local Maven development

With Java 25 and Maven 3.9+ installed:

```bash
mvn verify
```

The test suite covers deterministic generation, JSON round trips, transform and metadata workload rules, percentiles/medians, validation, configuration, retained-topic skip decisions, HTML generation, higher/lower winner rules, ties, and invalid comparisons. Kafka integration itself requires a real cluster and is intentionally not mocked.

## Output files

- `raw-<runId>.json`: one machine-readable measured iteration.
- `summary.json`: all measured results and skipped scenarios.
- `index.html`: self-contained static comparison report.

Passwords are excluded. Mount `/app/results` to persist files after container removal.

## Known limitations

- Results are specific to the supplied cluster, network, container limits, topic state, and configuration; they are not universal performance claims.
- Publish latency is publish-to-observe rather than broker-ack latency so the two implementations use one comparable definition.
- Process RAM uses Linux resident set size in the container; the fallback outside Linux is used JVM heap.
- CPU and RAM use time-aligned totals across processor worker services; the separate coordinator's generator, observer, report server, and orchestration overhead are excluded.
- Latency percentiles are based on at most 100,000 evenly spaced samples per stage for bounded memory use.
- Worker control uses unauthenticated HTTP on the private container network; do not publish worker ports.
- Retained topics accumulate benchmark records and require an external retention policy appropriate for the test environment.
- Metadata state is intentionally volatile and changelogging is disabled for parity; crash recovery and state restoration are not measured.
- The basic HTTP server has no authentication; expose it only on a trusted network.
