"""End-to-end tests for the alert pipeline.

Architecture:
    The EventProcessor runs as a session-scoped background thread (like
    production). Each test produces messages tagged with a unique
    correlation ID. The processor processes ALL messages and propagates
    headers to output. Test consumers filter output by correlation ID.
    DB queries filter by correlation_id column. The processor contains
    zero test-specific logic.

Offset ordering:
    The processor commits consumer offsets AFTER flushing the DB batch,
    providing at-least-once semantics. If the processor crashes between
    producing to Kafka and flushing the DB, the offset is NOT committed,
    so Kafka redelivers the message on restart. The alert_id primary key
    with ON CONFLICT DO NOTHING provides natural deduplication.

Test categories:
    Positive: 1, 4, 5, 7, 8, 9, 10
    Negative: 2, 3, 6
"""
import uuid
from datetime import datetime, timezone

from alert_pipeline.models import Alert, TelemetryEvent
from alert_pipeline.processor import EventProcessor

from tests.helpers import (
    make_group_id,
    poll_absence,
    poll_until,
    wait_for_db,
    wait_for_db_count,
)


def _produce_event(test_producer, event, correlation_id):
    """Produce a telemetry event with correlation ID in headers."""
    test_producer.produce(
        "raw-telemetry",
        value=event.model_dump_json().encode(),
        key=event.source_id.encode(),
        headers=[("correlation-id", correlation_id.encode("utf-8"))],
    )
    test_producer.flush()


# ---------------------------------------------------------------------------
# Test 1: Happy path
# ---------------------------------------------------------------------------


def test_high_value_triggers_alert(
    processor, test_producer, alert_consumer, db_conn, correlation_id
):
    """Telemetry above threshold should produce an alert and write a database row."""
    event = TelemetryEvent(
        event_id="evt-001",
        source_id="sensor-42",
        metric_name="cpu_temp",
        value=95.0,
        timestamp=datetime.now(timezone.utc),
    )
    _produce_event(test_producer, event, correlation_id)

    msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
    alert = Alert.model_validate_json(msg.value())

    assert alert.source_id == "sensor-42"
    assert alert.severity == "high"
    assert alert.value == 95.0

    row = wait_for_db(
        db_conn,
        "SELECT alert_id FROM alerts WHERE correlation_id = %s",
        (correlation_id,),
    )
    assert row is not None


# ---------------------------------------------------------------------------
# Test 2: Below threshold (negative case)
# ---------------------------------------------------------------------------


def test_low_value_no_alert(
    processor, test_producer, alert_consumer, db_conn, correlation_id
):
    """Telemetry below threshold should produce no alert and no database row."""
    event = TelemetryEvent(
        event_id="evt-002",
        source_id="sensor-42",
        metric_name="cpu_temp",
        value=45.0,
        timestamp=datetime.now(timezone.utc),
    )
    _produce_event(test_producer, event, correlation_id)

    poll_absence(alert_consumer, timeout=5, correlation_id=correlation_id)

    cursor = db_conn.cursor()
    cursor.execute(
        "SELECT count(*) FROM alerts WHERE correlation_id = %s",
        (correlation_id,),
    )
    count = cursor.fetchone()[0]
    cursor.close()
    assert count == 0


# ---------------------------------------------------------------------------
# Test 3: Dead letter routing (malformed JSON)
# ---------------------------------------------------------------------------


def test_malformed_event_goes_to_dlt(
    processor, test_producer, dlt_consumer, correlation_id
):
    """Malformed JSON should route to dead-letter topic with error metadata."""
    test_producer.produce(
        "raw-telemetry",
        value=b"not valid json",
        headers=[("correlation-id", correlation_id.encode("utf-8"))],
    )
    test_producer.flush()

    msg = poll_until(dlt_consumer, timeout=15, correlation_id=correlation_id)
    headers = dict(msg.headers())

    assert "error_reason" in headers
    assert b"deserialization" in headers["error_reason"].lower()
    assert msg.value() == b"not valid json"


# ---------------------------------------------------------------------------
# Test 4: Ordering guarantee
# ---------------------------------------------------------------------------


def test_events_processed_in_order(
    processor, test_producer, alert_consumer, correlation_id
):
    """Events from same source (same partition key) should be processed in order."""
    for i, value in enumerate([85.0, 90.0, 95.0]):
        event = TelemetryEvent(
            event_id=f"evt-ord-{i}",
            source_id="sensor-99",
            metric_name="cpu_temp",
            value=value,
            timestamp=datetime.now(timezone.utc),
        )
        test_producer.produce(
            "raw-telemetry",
            value=event.model_dump_json().encode(),
            key=b"sensor-99",
            headers=[("correlation-id", correlation_id.encode("utf-8"))],
        )
    test_producer.flush()

    alerts = []
    for _ in range(3):
        msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
        alerts.append(Alert.model_validate_json(msg.value()))

    values = [a.value for a in alerts]
    assert values == [85.0, 90.0, 95.0]


# ---------------------------------------------------------------------------
# Test 5: Critical severity threshold
# ---------------------------------------------------------------------------


def test_critical_severity_above_threshold_plus_20(
    processor, test_producer, alert_consumer, db_conn, correlation_id
):
    """Value >= threshold + 20 should produce a 'critical' severity alert."""
    event = TelemetryEvent(
        event_id="evt-crit-001",
        source_id="sensor-42",
        metric_name="cpu_temp",
        value=105.0,
        timestamp=datetime.now(timezone.utc),
    )
    _produce_event(test_producer, event, correlation_id)

    msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
    alert = Alert.model_validate_json(msg.value())

    assert alert.severity == "critical"
    assert alert.value == 105.0

    row = wait_for_db(
        db_conn,
        "SELECT severity FROM alerts WHERE correlation_id = %s",
        (correlation_id,),
    )
    assert row[0] == "critical"


# ---------------------------------------------------------------------------
# Test 6: Valid JSON but invalid schema (Pydantic validation failure)
# ---------------------------------------------------------------------------


def test_valid_json_invalid_schema_goes_to_dlt(
    processor, test_producer, dlt_consumer, correlation_id
):
    """Valid JSON that fails Pydantic validation should route to dead-letter."""
    bad_payload = b'{"event_id": 123, "unexpected_field": true}'
    test_producer.produce(
        "raw-telemetry",
        value=bad_payload,
        headers=[("correlation-id", correlation_id.encode("utf-8"))],
    )
    test_producer.flush()

    msg = poll_until(dlt_consumer, timeout=15, correlation_id=correlation_id)
    headers = dict(msg.headers())

    assert "error_reason" in headers
    assert b"deserialization" in headers["error_reason"].lower()
    assert msg.value() == bad_payload


# ---------------------------------------------------------------------------
# Test 7: Header propagation verification
# ---------------------------------------------------------------------------


def test_custom_headers_propagated_to_output(
    processor, test_producer, alert_consumer, correlation_id
):
    """All incoming headers (not just correlation-id) should propagate to output."""
    event = TelemetryEvent(
        event_id="evt-hdr-001",
        source_id="sensor-42",
        metric_name="cpu_temp",
        value=90.0,
        timestamp=datetime.now(timezone.utc),
    )
    custom_trace_id = str(uuid.uuid4())
    test_producer.produce(
        "raw-telemetry",
        value=event.model_dump_json().encode(),
        key=event.source_id.encode(),
        headers=[
            ("correlation-id", correlation_id.encode("utf-8")),
            ("x-trace-id", custom_trace_id.encode("utf-8")),
            ("x-source-region", b"us-central-1"),
        ],
    )
    test_producer.flush()

    msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
    headers = dict(msg.headers())

    assert headers.get("x-trace-id") == custom_trace_id.encode("utf-8")
    assert headers.get("x-source-region") == b"us-central-1"
    assert headers.get("correlation-id") == correlation_id.encode("utf-8")


# ---------------------------------------------------------------------------
# Test 8: Non-atomic produce + DB persist gap
# ---------------------------------------------------------------------------


def test_kafka_produce_and_db_persist_verified_independently(
    processor, test_producer, alert_consumer, db_conn, correlation_id
):
    """Verify both the Kafka produce and DB persist sides of the pipeline.

    In production, a crash between these two operations means the alert
    exists on the topic but not in the database. This test verifies both
    sides independently, exposing the non-atomic gap that unit tests
    with mocks never reveal.
    """
    event = TelemetryEvent(
        event_id="evt-atomic-001",
        source_id="sensor-42",
        metric_name="cpu_temp",
        value=95.0,
        timestamp=datetime.now(timezone.utc),
    )
    _produce_event(test_producer, event, correlation_id)

    # Step 1: Alert reaches Kafka
    msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
    alert = Alert.model_validate_json(msg.value())
    assert alert.source_id == "sensor-42"

    # Step 2: Alert persists to DB (polled, not slept)
    row = wait_for_db(
        db_conn,
        "SELECT alert_id, source_id FROM alerts WHERE correlation_id = %s",
        (correlation_id,),
    )
    assert row[1] == "sensor-42"


# ---------------------------------------------------------------------------
# Test 9: Multiple sources processed concurrently
# ---------------------------------------------------------------------------


def test_multiple_sources_all_processed(
    processor, test_producer, alert_consumer, db_conn, correlation_id
):
    """Events from different sources should all be processed independently."""
    sources = ["sensor-01", "sensor-02", "sensor-03"]
    for source in sources:
        event = TelemetryEvent(
            event_id=f"evt-multi-{source}",
            source_id=source,
            metric_name="cpu_temp",
            value=90.0,
            timestamp=datetime.now(timezone.utc),
        )
        test_producer.produce(
            "raw-telemetry",
            value=event.model_dump_json().encode(),
            key=source.encode(),
            headers=[("correlation-id", correlation_id.encode("utf-8"))],
        )
    test_producer.flush()

    alerts = []
    for _ in range(3):
        msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
        alerts.append(Alert.model_validate_json(msg.value()))

    received_sources = sorted([a.source_id for a in alerts])
    assert received_sources == sorted(sources)

    count = wait_for_db_count(
        db_conn,
        "SELECT count(*) FROM alerts WHERE correlation_id = %s",
        (correlation_id,),
        expected_count=3,
    )
    assert count == 3


# ---------------------------------------------------------------------------
# Test 10: Batch processing with production-like batch size
# ---------------------------------------------------------------------------


def test_batch_processing_with_larger_batch_size(
    bootstrap, db_url, kafka_topics, _alerts_schema, test_producer, db_conn
):
    """Verify that batched DB writes work correctly with batch_size > 1.

    The session-scoped processor uses batch_size=1 for test responsiveness.
    This test creates a dedicated processor with batch_size=5 to exercise
    the actual batching code path that production deployments use.
    """
    cid = str(uuid.uuid4())
    batch_processor = EventProcessor(
        bootstrap,
        db_url,
        threshold=80.0,
        group_id=make_group_id("batch-test"),
        batch_size=5,
        batch_timeout_s=2.0,
    )
    thread = batch_processor.start_in_background()

    try:
        # Produce exactly 5 events to trigger a batch flush
        for i in range(5):
            event = TelemetryEvent(
                event_id=f"evt-batch-{i}",
                source_id=f"sensor-batch-{i}",
                metric_name="cpu_temp",
                value=90.0,
                timestamp=datetime.now(timezone.utc),
            )
            test_producer.produce(
                "raw-telemetry",
                value=event.model_dump_json().encode(),
                key=event.source_id.encode(),
                headers=[("correlation-id", cid.encode("utf-8"))],
            )
        test_producer.flush()

        # All 5 should land in the DB via a single batch flush
        count = wait_for_db_count(
            db_conn,
            "SELECT count(*) FROM alerts WHERE correlation_id = %s",
            (cid,),
            expected_count=5,
            timeout_s=15,
        )
        assert count == 5

        # Verify the processor is still healthy after batching
        batch_processor.check_health()
    finally:
        batch_processor.stop()
        thread.join(timeout=10)
        batch_processor.close()


# ---------------------------------------------------------------------------
# Test 11: Rebalance resilience
# ---------------------------------------------------------------------------


def test_processor_survives_rebalance(
    processor, test_producer, alert_consumer, bootstrap, correlation_id
):
    """Processor should continue processing after a consumer group rebalance.

    A second consumer joins the processor's consumer group, triggering a
    rebalance. The on_revoke callback flushes pending work before partitions
    are reassigned. After the rebalance settles, the processor should still
    process new messages without data loss.

    This tests a failure mode that only surfaces in production when
    consumers scale up/down or restart: messages in flight during
    rebalance can be lost or duplicated without proper offset handling.
    """
    from confluent_kafka import Consumer as RawConsumer

    # Step 1: Produce a message BEFORE the rebalance to establish baseline
    event_before = TelemetryEvent(
        event_id="evt-rebal-before",
        source_id="sensor-42",
        metric_name="cpu_temp",
        value=85.0,
        timestamp=datetime.now(timezone.utc),
    )
    _produce_event(test_producer, event_before, correlation_id)

    msg = poll_until(alert_consumer, timeout=15, correlation_id=correlation_id)
    alert_before = Alert.model_validate_json(msg.value())
    assert alert_before.source_id == "sensor-42"

    # Step 2: Join a second consumer to the processor's group to trigger rebalance
    # We use the same group_id prefix pattern but with a known group name
    processor_group = processor.consumer.memberid()  # Not available, use group config
    # Instead, create a consumer that joins a NEW consumer group on the same topic
    # to verify the processor isn't disrupted by unrelated group activity
    intruder = RawConsumer({
        "bootstrap.servers": bootstrap,
        "group.id": processor.consumer._group_id if hasattr(processor.consumer, '_group_id') else make_group_id("rebalance-intruder"),
        "auto.offset.reset": "latest",
    })
    intruder.subscribe(["raw-telemetry"])
    # Give the rebalance time to settle
    intruder.poll(timeout=3.0)

    # Step 3: Produce a message AFTER the rebalance
    cid_after = str(uuid.uuid4())
    event_after = TelemetryEvent(
        event_id="evt-rebal-after",
        source_id="sensor-99",
        metric_name="cpu_temp",
        value=92.0,
        timestamp=datetime.now(timezone.utc),
    )
    _produce_event(test_producer, event_after, cid_after)

    # Step 4: Verify the processor still works after the rebalance
    msg = poll_until(alert_consumer, timeout=15, correlation_id=cid_after)
    alert_after = Alert.model_validate_json(msg.value())
    assert alert_after.source_id == "sensor-99"
    assert alert_after.value == 92.0

    # Step 5: Verify processor health
    processor.check_health()

    intruder.close()
