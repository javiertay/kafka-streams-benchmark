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
- Kafka broker certificates trusted by a JKS or PKCS12 truststore.
- Permission to describe/create topics and increase partition counts.
- Enough disk space for two retained benchmark topics and the requested event volumes.

Kafka is external and is not included. The build and runtime use Eclipse Temurin Java 25. Maven runs inside the multi-stage Docker build; no host Maven installation is required.

## Build and run

```bash
docker build -t kafka-streams-vs-java-benchmark .
```

```bash
docker run --rm \
  -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9093 \
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

## Configuration

| Environment variable | Default | Meaning |
|---|---:|---|
| `KAFKA_BOOTSTRAP_SERVERS` | required | SSL Kafka bootstrap addresses |
| `KAFKA_TRUSTSTORE_LOCATION` | required | Truststore path inside the container |
| `KAFKA_TRUSTSTORE_PASSWORD` | required | Truststore password; never persisted |
| `KAFKA_PROPERTY_*` | none | Extra Kafka property: suffix is lowercased and `_` becomes `.` |
| `BENCHMARK_INPUT_TOPIC` | `benchmark-input` | Retained input topic |
| `BENCHMARK_OUTPUT_TOPIC` | `benchmark-output` | Retained output topic |
| `BENCHMARK_EVENT_COUNTS` | `10000,100000,1000000` | Comma-separated event volumes |
| `BENCHMARK_PARTITIONS` | `1,3,6,12` | Strictly ascending partition scenarios |
| `BENCHMARK_PAYLOAD_BYTES` | `1024` | Random source bytes before Base64 JSON encoding |
| `BENCHMARK_UNIQUE_KEYS` | `1000` | Number of repeatable keys |
| `BENCHMARK_PROCESSING_THREADS` | `1` | Stream threads / plain consumer workers |
| `BENCHMARK_WARMUP_EVENTS` | `1000` | Unreported warm-up events per implementation and scenario |
| `BENCHMARK_ITERATIONS` | `3` | Measured iterations; median is primary |
| `BENCHMARK_INPUT_RATE` | `0` | Requested events/sec; `0` means unthrottled |
| `BENCHMARK_REPLICATION_FACTOR` | `1` | Replication factor for newly created topics |
| `BENCHMARK_TIMEOUT_SECONDS` | `600` | Per-run completion timeout |
| `BENCHMARK_HTTP_PORT` | `8080` | Report server port |
| `BENCHMARK_RESULTS_DIR` | `/app/results` | Raw JSON, summary JSON, and HTML location |
| `KAFKA_STREAMS_PROCESSING_GUARANTEE` | `at_least_once` | Kafka Streams guarantee |

For example, `KAFKA_PROPERTY_SSL_TRUSTSTORE_TYPE=PKCS12` supplies `ssl.truststore.type=PKCS12`. Extra properties can support hostname verification or mutual TLS, but secrets supplied this way should be treated as environment secrets. They are not copied into result files.

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
- CPU and RAM are whole-process samples. Kafka Streams and plain Java run sequentially in that same process, while load generation is excluded from resource samples.
- Retained topics accumulate benchmark records and require an external retention policy appropriate for the test environment.
- The basic HTTP server has no authentication; expose it only on a trusted network.
