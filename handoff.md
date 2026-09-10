# Current objective

Deliver a maintainable Maven/Java 25 benchmark that compares Kafka Streams with plain Kafka consumers using exactly 500,000 inputs per measured run, partition variations, and horizontal processor-service scaling.

# Completed work

- Uses `BENCHMARK_EVENT_COUNT` (500,000 by default) as the exact workload size for every measured run. The full JSON input set is preloaded and flushed before processor timing begins; generation is not measured.
- Uses a fixed `BENCHMARK_WARMUP_EVENT_COUNT`, with no measurement-duration or requested-input-rate scenario axes.
- Separated coordinator work (generation, observation, reporting) from processor resources.
- Added six Compose worker services. Each active worker is a separate JVM/container with one Kafka Streams instance/stream thread or one conventional consumer/producer pair.
- Workers share the run-specific Streams application ID or plain consumer group. Each group's starting offsets are prepared before its fixed input set is preloaded.
- Aggregated worker counters, latency samples, CPU, RAM, and GC data; recorded per-service consumption distribution.
- Bounded each stage to 100,000 evenly spaced latency samples for large sustained runs.
- Added tabbed HTML configuration navigation and documented how to interpret fixed-workload completion time and throughput.
- Added concise coordinator and worker lifecycle logs for matrix/scenario progress, input preloading, measurement start, fixed-input processing, worker shutdown, report writes, validation, failures, and unambiguous final completion.
- Fixed multi-service startup: workers now start concurrently, startup uses `BENCHMARK_TIMEOUT_SECONDS`, repeated start requests for the same run are idempotent, and partial startup failures clean up workers that already started.
- Fixed conventional consumers with more services than partitions: a worker is ready after its first successful group poll, even when Kafka correctly assigns it no partition.
- Added an HTML overall summary that declares whether Kafka Streams or plain Java did better by counting valid configuration wins using median total processing throughput; ties and excluded invalid comparisons are shown explicitly.
- Reduced the partition/service matrix to meaningful combinations only: service counts greater than requested partitions are omitted, leaving `1/1`, `3/1`, `3/3`, `6/1`, `6/3`, and `6/6` for the Compose defaults.
- Split the two worker implementations into presentation-friendly `KafkaStreamsProcessor.java` and `TraditionalKafkaProcessor.java` files. Removed the empty `StreamsRunner`/`PlainRunner` subclasses and kept orchestration in the shared `DistributedRunner`.
- Replaced the single local broker with a configurable one-to-three-broker plaintext KRaft cluster. Compose defaults to three active brokers; `KAFKA_BROKER_COUNT=1` or `2` leaves higher-numbered broker containers idle.
- Made benchmark topic replication default to the configured broker count, configured local minimum ISR as one for a single broker and two otherwise, and reject replication factors above the declared broker count.
- Added a coordinator preflight that waits for exactly `KAFKA_BROKER_COUNT` registered brokers before any scenario starts.
- Added broker count and replication factor to safe result configuration and to the HTML report header so one- and three-broker verdicts remain visibly separate.
- Reduced the matrix to one configurable fixed count of 500,000 inputs, leaving six meaningful partition/service configurations.
- Added `BENCHMARK_PROCESSING_MODE=transform|metadata`. Compose defaults to the metadata workload, which injects deterministic duplicates, deduplicates the latest event per key, aggregates accepted updates by key and one-second logical window, and suppresses intermediate output until per-partition completion markers arrive.
- Implemented equivalent bounded in-memory metadata state in Kafka Streams task stores and plain Java maps, with Kafka Streams changelogging disabled so this is a processing benchmark rather than an unequal recovery benchmark.
- Results now distinguish generated inputs from expected outputs; validation requires every input consumed, every expected output published/observed exactly once, and every final aggregate sequence/count to match the generator's independent expectation.
- Fixed a conventional-consumer startup race uncovered by strict validation. The current fixed-input design prepares committed group offsets before preloading, then lets all workers join and consume the already-complete backlog.
- Aligned processing timing boundaries: both implementations are timed from JSON decoding through business logic and output handoff. Metadata final aggregate flushing is timed separately per partition marker.
- Corrected stage throughput to count the intervals between first and last events, and changed headline/summary throughput to consumed inputs over total elapsed time so metadata suppression does not make low output cardinality look like low processing capacity.
- Publishing throughput now uses the coordinator observer's monotonic clock; cross-worker ingestion and processing windows retain wall-clock boundaries so timestamps can be merged across service JVMs on the same host.

# Files changed

- Runtime/configuration: `compose.yaml`, `src/main/java/benchmark/Config.java`, `BenchmarkOrchestrator.java`, `Main.java`
- Distributed processing: `DistributedRunner.java`, `DistributedWorkers.java`, `ProcessorSession.java`, `WorkerServer.java`, `KafkaStreamsProcessor.java`, `TraditionalKafkaProcessor.java`, `KafkaSupport.java`
- Measurement/reporting: `RunSupport.java`, `OutputCollector.java`, `Statistics.java`, `BenchmarkResult.java`, `ReportWriter.java`
- Shared workload/tests/docs: `Workload.java`, `ConfigTest.java`, `WorkloadTest.java`, `ValidationTest.java`, `StatisticsTest.java`, `ReportWriterTest.java`, `README.md`, `handoff.md`

# Commands run and results

- Dockerized Maven `mvn -B verify` after timing corrections — passed with 25 tests.
- Isolated three-broker metadata timing smoke at 100 events/s for two seconds, one partition, and one service — passed for both implementations: 201 inputs consumed and all 161 expected aggregates published/observed exactly once. Results reported about 94 inputs/s end-to-end for both paths and separate metadata flush latency (30.99 ms Streams, 16.01 ms plain Java). Temporary containers, network, and override were removed.
- Isolated three-broker transform timing smoke with the same rate, duration, partition, and service count — passed for both implementations with all 201 inputs and outputs validated. Temporary containers, network, and override were removed.
- Final Dockerized Maven `mvn -B verify` after the stateful workload changes — passed with 19 production sources and 24 tests.
- Three-broker/three-partition/three-service metadata smoke at 100 events/s for two seconds — passed for both implementations: 201 inputs consumed exactly once and 17 final aggregates published/observed exactly once with zero incorrect aggregate values. HTML returned HTTP 200 and showed the metadata workload, expected-output row, and strict validation status.
- Three-broker/three-partition/three-service transform regression with the same input — passed for both implementations with 201 inputs and 201 outputs validated.
- `docker-compose -f compose.yaml config --quiet` — passed with both the default three-broker interpolation and `KAFKA_BROKER_COUNT=1`.
- Three-broker Compose smoke at 1k events/s, one partition, one service, and one measured second — passed for Kafka Streams and plain Java with 1,001/1,001 outputs validated. Coordinator detected three brokers and replication factor three; topic metadata showed replicas/ISR on brokers 1, 2, and 3 and minimum ISR two; report returned HTTP 200 and named the 3-broker/RF3 environment.
- Single-broker Compose smoke with the same workload — passed for both implementations with 1,001/1,001 outputs validated. Brokers 2 and 3 logged that they were disabled, coordinator detected one broker and replication factor one, topic metadata showed RF1/minimum ISR1, and the report named the 1-broker/RF1 environment.
- Final distributed Compose smoke test using a fresh plaintext broker, 1-second load at 10k events/s, 3 partitions, and 1/3 services — passed with 4 valid results and zero missing/duplicate/unexpected outputs.
- Final smoke distributions: Kafka Streams 3-service run consumed 3240/3361/3400; plain Java consumed 3400/3360/3239.
- Final smoke achieved about 10k events/s in every run. Window-boundary backlog/catch-up examples were 976/0.14s for one Kafka Streams service, 57/0.01s for one plain service, 3501/2.46s for three Streams services, and 326/0.03s for three plain services.
- Smoke report returned HTTP 200 with two configuration tabs, two panels, producer-flush time, backlog, and catch-up metrics.
- Lifecycle-log smoke test using an isolated plaintext stack, 1 second at 1k events/s, 1 partition, and 1 service — passed for both implementations with 1,001/1,001 outputs observed. Coordinator logs reached `[benchmark] COMPLETED` and `[server] READY: benchmark complete`; worker logs reached `STOPPED` for both runs; the report returned HTTP 200.
- Worker-start regression smoke using 1 partition and 6 separate services — passed for both implementations with all six workers ready, 1,001/1,001 outputs observed, and five idle services per run as expected. This directly covers the former worker readiness timeout when service count exceeded partition count.
- Post-refactor Compose smoke using 1 partition, 1 service, and a 1-second 1k events/s window — passed for both implementations with 1,001/1,001 outputs validated and a completed report.
- Temporary smoke override, containers, and network were removed.

# Known issues / unverified

- The complete default 500,000-record × 1/3/6 partitions × 1/3/6 services matrix has not been run; only earlier focused distributed smoke runs are integration-verified.
- Input preloading is excluded from all processor timing and throughput results.
- No SSL Kafka cluster was supplied, so SSL integration remains unverified.
- The two-broker option is structurally configured and Compose-validated but has not received a full runtime smoke test; the requested one- and three-broker comparison modes have.
- Metadata state is intentionally volatile and Kafka Streams changelogging is disabled for parity; crash recovery and restoration behavior remain outside the benchmark.
- Merged multi-worker ingestion and processing throughput use wall-clock timestamps because events cross JVMs; local processing, flush, publishing-window, and total durations use monotonic timers. Run distributed workers on clock-synchronized hosts if the topology is moved beyond one Docker host.

# Next recommended steps

1. Run the default local matrix when its several-minute runtime and broker disk usage are acceptable.
2. Increase the fixed event count on representative infrastructure for decision-grade sustained results.
3. Add committed-offset lag only if broker-level lag behavior is required beyond the current fixed-backlog completion measurement.
