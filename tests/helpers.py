"""Test helpers for Kafka e2e testing.

poll_until / poll_absence:
    These helpers filter Kafka messages by correlation-id header. Because
    test consumers use auto.offset.reset=earliest with unique consumer
    groups, they scan from the beginning of the topic log. For long-running
    CI environments where topics accumulate thousands of messages, consider
    switching test consumers to auto.offset.reset=latest with explicit
    seek_to_end() before each test to avoid O(n²) scanning overhead.

wait_for_db:
    Polls the database instead of sleeping a fixed duration. This detects
    results faster (typically <200ms) and avoids the time.sleep() anti-pattern
    that masks timing uncertainty.
"""
import time
import uuid


def poll_until(consumer, timeout=10, correlation_id=None):
    """Poll consumer until a matching message arrives or timeout expires.

    When correlation_id is provided, skips messages that don't carry
    a matching 'correlation-id' header. This enables test isolation
    on shared topics: each test tags its messages with a unique ID,
    and the consumer ignores everything produced by other tests.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        if correlation_id is not None:
            headers = dict(msg.headers() or [])
            msg_cid = headers.get("correlation-id")
            if msg_cid != correlation_id.encode("utf-8"):
                continue

        return msg

    raise TimeoutError("No correlated message received before timeout")


def poll_absence(consumer, timeout=3, correlation_id=None):
    """Poll consumer and assert NO matching message arrives before timeout.

    Raises AssertionError if a message with the matching correlation_id
    appears. Used for negative tests (e.g., below-threshold events should
    not produce alerts).
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        msg = consumer.poll(timeout=1.0)
        if msg is None or msg.error():
            continue

        if correlation_id is not None:
            headers = dict(msg.headers() or [])
            msg_cid = headers.get("correlation-id")
            if msg_cid == correlation_id.encode("utf-8"):
                raise AssertionError(
                    f"Unexpected message with correlation_id={correlation_id}"
                )


def wait_for_db(db_conn, query, params, timeout_s=5, poll_interval_s=0.1):
    """Poll the database until a query returns a non-empty result.

    Replaces time.sleep() with explicit polling: faster detection
    (typically <200ms) and no fixed-duration waiting.

    Returns the first row, or raises TimeoutError.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        cursor = db_conn.cursor()
        cursor.execute(query, params)
        row = cursor.fetchone()
        cursor.close()
        if row is not None:
            return row
        time.sleep(poll_interval_s)

    raise TimeoutError(f"DB query returned no results after {timeout_s}s")


def wait_for_db_count(db_conn, query, params, expected_count, timeout_s=5, poll_interval_s=0.1):
    """Poll the database until a COUNT query reaches the expected value.

    Returns the count, or raises TimeoutError.
    """
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        cursor = db_conn.cursor()
        cursor.execute(query, params)
        count = cursor.fetchone()[0]
        cursor.close()
        if count >= expected_count:
            return count
        time.sleep(poll_interval_s)

    raise TimeoutError(
        f"DB count {count} did not reach {expected_count} after {timeout_s}s"
    )


def make_group_id(prefix: str = "test") -> str:
    """Generate a unique consumer group ID using UUID."""
    return f"{prefix}-{uuid.uuid4()}"
