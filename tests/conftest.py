"""Pytest fixtures for Kafka e2e testing.

Fixture scoping strategy:
    Session-scoped: Kafka container, PostgreSQL container, topic creation,
    DB schema, and the EventProcessor. These are expensive to create (10-30s
    each) and are reused across all tests. The processor runs continuously
    in a background thread, mirroring production deployment.

    Function-scoped: correlation_id, db_conn, test_producer, alert_consumer,
    dlt_consumer. These provide per-test isolation. Each test gets a unique
    correlation ID that tags its messages, and all assertions filter by this
    ID on both Kafka consumers and DB queries.

Consumer group isolation:
    The session-scoped processor and the function-scoped test consumers use
    SEPARATE consumer groups (different group_id values). This is intentional:
    test consumers reading from the output topics must not trigger rebalances
    on the processor's consumer group. Each alert_consumer and dlt_consumer
    gets its own unique group_id via make_group_id(), completely independent
    of the processor's group.

Transaction isolation:
    The processor and test code use separate psycopg2 connections to the same
    PostgreSQL instance. PostgreSQL's default READ COMMITTED isolation means
    test queries see processor writes once committed. The wait_for_db helper
    polls until the commit is visible, avoiding time.sleep() guesswork.
"""
import os
import sys
import time
import uuid

import psycopg2
import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from alert_pipeline.processor import EventProcessor
from tests.helpers import make_group_id


# ---------------------------------------------------------------------------
# Session-scoped: expensive infrastructure (start once, reuse across all tests)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_container():
    container = KafkaContainer("confluentinc/cp-kafka:7.6.0")
    container.start()

    bootstrap = container.get_bootstrap_server()
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for _ in range(30):
        try:
            admin.list_topics(timeout=2)
            break
        except Exception:
            time.sleep(1)
    else:
        raise RuntimeError("Kafka broker failed to become ready")

    yield container
    container.stop()


@pytest.fixture(scope="session")
def postgres_container():
    container = PostgresContainer("postgres:16-alpine")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def bootstrap(kafka_container):
    return kafka_container.get_bootstrap_server()


@pytest.fixture(scope="session")
def db_url(postgres_container):
    return postgres_container.get_connection_url().replace("+psycopg2", "")


@pytest.fixture(scope="session")
def kafka_topics(bootstrap):
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = [
        NewTopic("raw-telemetry", num_partitions=1, replication_factor=1),
        NewTopic("alerts", num_partitions=1, replication_factor=1),
        NewTopic("dead-letter", num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(topics)
    for topic, future in futures.items():
        try:
            future.result(timeout=10)
        except Exception:
            pass
    time.sleep(1)
    return bootstrap


@pytest.fixture(scope="session")
def _alerts_schema(db_url):
    """Create the alerts table once per session.

    Includes correlation_id column for traceability and test isolation,
    with an index for query performance as the table grows across test runs.
    ON CONFLICT (alert_id) DO NOTHING in the processor provides natural
    deduplication if messages are replayed after a crash.
    """
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id VARCHAR(255) PRIMARY KEY,
            source_id VARCHAR(255) NOT NULL,
            metric_name VARCHAR(255) NOT NULL,
            value FLOAT NOT NULL,
            threshold FLOAT NOT NULL,
            severity VARCHAR(50) NOT NULL,
            triggered_at TIMESTAMPTZ NOT NULL,
            correlation_id VARCHAR(255)
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_alerts_correlation_id
        ON alerts (correlation_id)
    """)
    conn.commit()
    cursor.close()
    conn.close()


@pytest.fixture(scope="session")
def processor(bootstrap, db_url, kafka_topics, _alerts_schema):
    """Session-scoped processor running in a background thread.

    Mirrors production: starts once, runs continuously. Tests isolate
    through correlation IDs, not by restarting the processor.

    batch_size=1 ensures each message flushes immediately for test
    responsiveness. Production deployments should use batch_size >= 10
    for throughput. See test_batch_processing for a dedicated batch test.
    """
    proc = EventProcessor(
        bootstrap,
        db_url,
        threshold=80.0,
        group_id=make_group_id("session-processor"),
        batch_size=1,
        batch_timeout_s=1.0,
    )
    thread = proc.start_in_background()
    yield proc
    proc.stop()
    thread.join(timeout=10)
    proc.close()


# ---------------------------------------------------------------------------
# Health check: detect silent processor thread failures after each test
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _check_processor_health(processor):
    """Runs after every test. Fails the test if the processor thread died."""
    yield
    processor.check_health()


# ---------------------------------------------------------------------------
# Function-scoped: per-test isolation via correlation IDs
# ---------------------------------------------------------------------------


@pytest.fixture(scope="function")
def correlation_id():
    """Unique ID for this test run. Tags every message so consumers can
    filter out messages from other tests sharing the same topics, and
    DB queries can scope to this test's data only."""
    return str(uuid.uuid4())


@pytest.fixture(scope="function")
def db_conn(db_url, _alerts_schema):
    """Database connection scoped per test.

    Each test queries by its own correlation_id for isolation. No blanket
    DELETE needed. PostgreSQL READ COMMITTED isolation ensures this connection
    sees processor writes once they are committed.
    """
    conn = psycopg2.connect(db_url)
    yield conn
    conn.close()


@pytest.fixture(scope="function")
def test_producer(kafka_topics):
    """Producer for test code to send messages to the input topic."""
    p = Producer({"bootstrap.servers": kafka_topics})
    yield p
    p.flush()


@pytest.fixture(scope="function")
def alert_consumer(kafka_topics):
    c = Consumer({
        "bootstrap.servers": kafka_topics,
        "group.id": make_group_id("test-alerts"),
        "auto.offset.reset": "earliest",
    })
    c.subscribe(["alerts"])
    yield c
    c.close()


@pytest.fixture(scope="function")
def dlt_consumer(kafka_topics):
    c = Consumer({
        "bootstrap.servers": kafka_topics,
        "group.id": make_group_id("test-dlt"),
        "auto.offset.reset": "earliest",
    })
    c.subscribe(["dead-letter"])
    yield c
    c.close()
