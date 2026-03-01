# Kafka Testing Tutorial

Production-grade end-to-end testing for Kafka-based applications using **pytest**, **testcontainers**, and **confluent-kafka**.

This repository is the companion code for the article: **"End-to-End Testing Across Kafka: Building and Testing an Alert Pipeline from Scratch"**

## What This Demonstrates

A telemetry alert pipeline that shows how to test the complete message flow across Kafka topics, transformations, and database side effects using production patterns:

```
raw-telemetry (topic) → EventProcessor → alerts (topic) + PostgreSQL
                                       → dead-letter (topic, on failure)
```

### Key Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| **Correlation ID isolation** | Headers on all messages + DB column | Tests share topics but filter by unique ID per test |
| **Session-scoped processor** | conftest.py fixture | Processor runs continuously like production, not restarted per test |
| **Manual offset commits** | Processor commits after DB flush | At-least-once semantics: no data loss on crash |
| **Rebalance listener** | on_revoke callback | Flushes pending work before partitions are reassigned |
| **Dead letter routing** | Processor error handling | Malformed messages preserved with error metadata in headers |
| **Header propagation** | Processor passes all headers through | Enables distributed tracing (OpenTelemetry compatible) |
| **Batched DB writes** | executemany + ON CONFLICT DO NOTHING | Throughput optimization with idempotent replay |
| **DB polling helpers** | wait_for_db / wait_for_db_count | Replaces time.sleep() with explicit condition polling |
| **Processor health checks** | autouse fixture + check_health() | Detects silent background thread failures |

### What the Processor Does NOT Contain

Zero test-specific logic. No correlation ID filtering, no `max_messages` parameter, no test flags. The processor behaves identically in tests and production. All test isolation lives in the test infrastructure.

## Quick Start

### Prerequisites
- Python 3.11+
- Docker (for testcontainers and local dev)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Run Tests

```bash
git clone https://github.com/drbothen/kafka-testing-tutorial.git
cd kafka-testing-tutorial

uv sync --extra test

# Docker must be running - testcontainers starts Kafka + PostgreSQL automatically
uv run pytest tests/ -v
```

### Local Development

For manual testing and exploration, use Docker Compose:

```bash
# Start Kafka (KRaft mode, no ZooKeeper) + PostgreSQL
docker compose up -d

# Produce sample telemetry events
uv run python -m alert_pipeline.producer --count 10

# In another terminal, consume alerts
uv run python -m alert_pipeline.consumer

# Stop when done
docker compose down
```

## Test Suite

11 tests covering positive paths, negative paths, error routing, and production failure modes:

| # | Test | Type | What It Verifies |
|---|------|------|-----------------|
| 1 | `test_high_value_triggers_alert` | Positive | Event above threshold → alert on topic + DB row |
| 2 | `test_low_value_no_alert` | Negative | Event below threshold → no alert, no DB row |
| 3 | `test_malformed_event_goes_to_dlt` | Negative | Invalid JSON → dead-letter with error metadata |
| 4 | `test_events_processed_in_order` | Positive | Same partition key → ordered processing |
| 5 | `test_critical_severity_above_threshold_plus_20` | Positive | Threshold + 20 → critical severity branch |
| 6 | `test_valid_json_invalid_schema_goes_to_dlt` | Negative | Valid JSON, bad schema → dead-letter |
| 7 | `test_custom_headers_propagated_to_output` | Positive | Custom headers survive the processor |
| 8 | `test_kafka_produce_and_db_persist_verified_independently` | Positive | Non-atomic Kafka + DB gap exposed |
| 9 | `test_multiple_sources_all_processed` | Positive | Different partition keys all processed |
| 10 | `test_batch_processing_with_larger_batch_size` | Positive | Batched DB writes with batch_size=5 |
| 11 | `test_processor_survives_rebalance` | Positive | Processing continues after consumer rebalance |

## Architecture

### Fixture Scoping

```
Session-scoped (start once, reuse):          Function-scoped (fresh per test):
├── KafkaContainer (cp-kafka:7.6.0)          ├── correlation_id (uuid4)
├── PostgresContainer (postgres:16)          ├── db_conn (psycopg2, yield+close)
├── Topic creation (AdminClient)             ├── test_producer
├── DB schema + index                        ├── alert_consumer (unique group)
└── EventProcessor (background thread)       └── dlt_consumer (unique group)
```

### Consumer Group Isolation

The processor and test consumers use **separate consumer groups**. Test consumers reading from output topics will not trigger rebalances on the processor. Each alert_consumer and dlt_consumer gets its own unique group_id.

### Delivery Semantics

At-least-once. Consumer offsets are committed only AFTER the database batch is flushed. If the processor crashes between producing to Kafka and flushing the DB, the offset has NOT been committed, so Kafka redelivers. The `alert_id` primary key with `ON CONFLICT DO NOTHING` provides natural deduplication on replay.

## Project Structure

```
├── src/alert_pipeline/
│   ├── models.py        # TelemetryEvent, Alert (Pydantic v2)
│   ├── processor.py     # EventProcessor (consume → transform → produce + persist)
│   ├── producer.py      # CLI: generate telemetry events
│   └── consumer.py      # CLI: read alerts
├── tests/
│   ├── conftest.py      # Fixtures: containers, processor, isolation
│   ├── helpers.py       # poll_until, poll_absence, wait_for_db, make_group_id
│   └── test_pipeline.py # 11 e2e test cases
├── docker-compose.yml   # Local dev (KRaft Kafka + PostgreSQL)
└── .github/workflows/   # CI (GitHub Actions)
```

## Validated Through

This codebase was validated through 4 rounds of production engineering review covering:
- Offset commit ordering and at-least-once guarantees
- Consumer rebalance handling with on_revoke callbacks
- Thread safety (threading.Event, consumer.wakeup())
- Test isolation patterns (correlation IDs vs unique topics)
- Database transaction isolation (READ COMMITTED, connection scoping)
- Batch processing correctness (executemany, ON CONFLICT)
- Graceful shutdown coordination

## License

MIT
