# Current objective

Deliver a standalone Maven/Java 25 project that benchmarks Kafka Streams against plain `KafkaConsumer`/`KafkaProducer` processing in one Docker image and serves an understandable static HTML report.

# Completed work

- Implemented SSL configuration, retained two-topic creation/growth, ascending partition handling, deterministic JSON workloads, unique run isolation, warm-ups, iterations, and optional fixed input rate.
- Implemented sequential Kafka Streams and plain Java runners using the same codec, transformation, topics, acknowledgements, concurrency, observer, validation, and metrics.
- Added CPU/RAM and JVM/GC details, JSON persistence, a plain-English static report, winner/tie/invalid rules, and a JDK HTTP server.
- Added a Java 25 multi-stage Maven Docker build, tests, and setup documentation.
- Centralized Kafka Streams, consumer, and producer property construction in `KafkaSupport`; split both runners into named topology/worker/processing methods.
- Documented the exact Kafka Streams and conventional client configuration, offset isolation, threading, publishing, and observation flows in `README.md`.
- Added `KAFKA_SECURITY_PROTOCOL` with validated support for `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL`; truststore settings are required only by SSL protocols.
- Added `compose.yaml` for a two-service local load test: a single-node Kafka 4.1.0 KRaft plaintext broker and the benchmark configured with a rate/partition/consumer matrix.
- Fixed the output-observer startup race found by the first Compose run by resolving lazy `seekToEnd` positions before allowing processing to start.
- Extended the benchmark to a rate × partition × consumer-count matrix. Local Compose requests 100k and 1m events/s across 1/3/6 partitions and 1/3/6 consumers using 100k-event runs.
- Fixed fixed-rate semantics so the workload generator runs concurrently with active consumers instead of preloading the topic before measurement.

# Files changed

- Build/container: `pom.xml`, `Dockerfile`, `compose.yaml`, `.dockerignore`, `.gitignore`
- Application and tests: `src/main/java/benchmark/*.java`, `src/test/java/benchmark/*.java`
- Documentation: `README.md`, `handoff.md`

# Commands run and results

- `java -version` — passed; Oracle Java 25.0.4.
- Apache Maven 3.9.11 `mvn -B verify` — passed after the matrix changes; 15 production sources compiled, 18 tests passed, and `target/benchmark.jar` was produced.
- `java -jar target/benchmark.jar` without environment — passed expected validation; exited 2 for missing `KAFKA_BOOTSTRAP_SERVERS`.
- `docker-compose -f compose.yaml config --quiet` — passed; the two-service Compose model resolved successfully.
- `docker-compose -f compose.yaml up --build -d` — passed; built the Java 25 image, ran all 18 tests in the image build, started Kafka healthy, and completed the expanded plaintext matrix.
- Expanded local load results — 36 measured implementation results across 18 matched scenarios, with zero invalid or skipped runs and zero missing, duplicate, or unexpected outputs. At a requested 100k events/s, achieved rates ranged from about 54k to 100k events/s; at a requested 1m events/s, achieved rates ranged from about 157k to 383k events/s. `http://localhost:8080` returned HTTP 200 with the generated report.
- `docker-compose -f compose.yaml down` — passed; removed the temporary containers and network.

# Known issues / unverified

- The local plaintext flow is verified, including topic creation, partition increases through 1/3/6, 1/3/6 consumer instances for both implementations, validation, JSON/HTML generation, and HTTP serving.
- No SSL Kafka cluster was supplied, so the external SSL flow remains integration-unverified.
- Publishing latency is comparable publish-to-observe latency, not broker acknowledgement latency; Kafka Streams has no per-record producer callback.
- Fixed-rate results persist requested and achieved input rate but do not currently report consumer lag.
- Consumer instances are concurrent stream threads or `KafkaConsumer` workers in one benchmark JVM; separate JVM/container replicas are not orchestrated.

# Next recommended steps

1. Run an SSL-cluster smoke test before increasing the external-cluster matrix.
2. Use at least one million events per run when evaluating whether one million events/s can be sustained rather than briefly attempted.
3. Add consumer-lag collection if fixed-rate tests require it.
