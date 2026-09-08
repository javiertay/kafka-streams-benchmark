# Current objective

Deliver a standalone Maven/Java 25 project that benchmarks Kafka Streams against plain `KafkaConsumer`/`KafkaProducer` processing in one Docker image and serves an understandable static HTML report.

# Completed work

- Implemented SSL configuration, retained two-topic creation/growth, ascending partition handling, deterministic JSON workloads, unique run isolation, warm-ups, iterations, and optional fixed input rate.
- Implemented sequential Kafka Streams and plain Java runners using the same codec, transformation, topics, acknowledgements, concurrency, observer, validation, and metrics.
- Added CPU/RAM and JVM/GC details, JSON persistence, a plain-English static report, winner/tie/invalid rules, and a JDK HTTP server.
- Added a Java 25 multi-stage Maven Docker build, tests, and setup documentation.

# Files changed

- Build/container: `pom.xml`, `Dockerfile`, `.dockerignore`, `.gitignore`
- Application and tests: `src/main/java/benchmark/*.java`, `src/test/java/benchmark/*.java`
- Documentation: `README.md`, `handoff.md`

# Commands run and results

- `java -version` — passed; Oracle Java 25.0.4.
- Apache Maven 3.9.11 `mvn -B verify` — passed; 15 production sources compiled, 13 tests passed, and `target/benchmark.jar` was produced.
- `java -jar target/benchmark.jar` without environment — passed expected validation; exited 2 for missing `KAFKA_BOOTSTRAP_SERVERS`.
- `docker build -t kafka-streams-vs-java-benchmark .` — could not complete because the Docker Desktop Linux engine was not running.

# Known issues / unverified

- No SSL Kafka cluster was supplied, so live topic management, partition increases, both processors, result validation/persistence, and post-run HTTP serving remain integration-unverified.
- Docker build/start remains unverified until Docker Desktop's Linux engine is running.
- Publishing latency is comparable publish-to-observe latency, not broker acknowledgement latency; Kafka Streams has no per-record producer callback.
- Fixed-rate results persist requested and achieved input rate but do not currently report consumer lag.

# Next recommended steps

1. Start Docker Desktop in Linux-container mode and build the image.
2. Run an SSL-cluster smoke test with one partition, 100 events, zero warm-up, and one iteration.
3. Inspect the HTML/JSON output, topic retention, and validation counts before increasing the matrix.
