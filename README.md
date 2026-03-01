# Kafka Testing Tutorial

End-to-end testing for Kafka-based applications using **pytest**, **testcontainers**, and **confluent-kafka**.

This repository is the companion code for the article: **"End-to-End Testing Across Kafka: Building and Testing an Alert Pipeline from Scratch"**

## What This Demonstrates

A simple alert pipeline that shows how to test the complete message flow across Kafka topics, transformations, and database side effects:

```
raw-telemetry (topic) → EventProcessor → alerts (topic) + PostgreSQL
                                       → dead-letter (topic, on failure)
```

The project includes:
- **Pydantic models** for type-safe event serialization
- **EventProcessor** that consumes, evaluates thresholds, and produces alerts
- **pytest + testcontainers** for e2e tests against real Kafka and PostgreSQL
- **Dead letter topic** pattern for error routing
- **GitHub Actions** CI workflow

## Quick Start

### Prerequisites
- Python 3.11+
- Docker (for testcontainers and local dev)
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

### Run Tests

```bash
# Clone the repo
git clone https://github.com/drbothen/kafka-testing-tutorial.git
cd kafka-testing-tutorial

# Install dependencies
uv sync --extra test

# Run the e2e tests (Docker must be running)
uv run pytest tests/ -v
```

Tests automatically start Kafka and PostgreSQL containers via testcontainers. No Docker Compose needed.

### Local Development

For manual testing and exploration, use Docker Compose:

```bash
# Start Kafka (KRaft mode) + PostgreSQL
docker compose up -d

# Produce sample telemetry events
uv run python -m alert_pipeline.producer --count 10

# In another terminal, consume alerts
uv run python -m alert_pipeline.consumer

# Stop when done
docker compose down
```

## Test Cases

| Test | What It Verifies |
|------|-----------------|
| `test_high_value_triggers_alert` | Event above threshold → alert on topic + row in DB |
| `test_low_value_no_alert` | Event below threshold → no alert, no DB row |
| `test_malformed_event_goes_to_dlt` | Bad JSON → dead-letter topic with error metadata |
| `test_events_processed_in_order` | Same partition key → ordered processing |

## Project Structure

```
├── src/alert_pipeline/
│   ├── models.py        # TelemetryEvent, Alert (Pydantic)
│   ├── processor.py     # EventProcessor (consume → transform → produce + persist)
│   ├── producer.py      # CLI: generate telemetry events
│   └── consumer.py      # CLI: read alerts
├── tests/
│   ├── conftest.py      # pytest fixtures (testcontainers)
│   ├── helpers.py       # poll_until async helper
│   └── test_pipeline.py # 4 e2e test cases
├── docker-compose.yml   # Local dev (KRaft Kafka + PostgreSQL)
└── .github/workflows/   # CI
```

## License

MIT
