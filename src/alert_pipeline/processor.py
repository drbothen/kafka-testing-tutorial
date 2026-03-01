"""Event processor for the alert pipeline.

Consumes telemetry events from Kafka, evaluates thresholds, produces alerts,
and persists to PostgreSQL with batched writes.

Delivery semantics:
    At-least-once. Consumer offsets are committed only AFTER the database
    batch is flushed. If the processor crashes between producing an alert
    to Kafka and flushing the DB batch, the offset has NOT been committed,
    so Kafka will redeliver the message on restart. This means:
    - Alerts on Kafka: at-least-once (produced and flushed immediately)
    - Alerts in DB: at-least-once (flushed before offset commit)
    - Duplicate processing is possible on restart; the DB schema uses
      alert_id as primary key for natural deduplication.

Batching:
    DB writes are accumulated in _db_batch and flushed when batch_size is
    reached or batch_timeout_s elapses on idle. In tests, batch_size=1
    disables accumulation for responsiveness. Production deployments should
    use batch_size >= 10 for throughput.

Header propagation:
    All incoming Kafka headers (including correlation-id for distributed
    tracing) are propagated to output messages. The processor extracts
    correlation-id and stores it in the DB correlation_id column for
    query filtering and traceability.

Thread safety:
    stop() uses threading.Event for safe cross-thread signaling.
    Exceptions in the processing loop are captured and exposed via
    check_health() so callers can detect silent thread failures.
"""
import json
import threading
import time
import uuid
from datetime import datetime, timezone

import psycopg2
from confluent_kafka import Consumer, Producer, KafkaError
from pydantic import ValidationError

from alert_pipeline.models import TelemetryEvent, Alert


class EventProcessor:

    def __init__(
        self,
        bootstrap_servers: str,
        db_url: str,
        threshold: float,
        group_id: str = "alert-processor",
        input_topic: str = "raw-telemetry",
        output_topic: str = "alerts",
        dlt_topic: str = "dead-letter",
        batch_size: int = 100,
        batch_timeout_s: float = 5.0,
    ):
        self.threshold = threshold
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.dlt_topic = dlt_topic
        self.batch_size = batch_size
        self.batch_timeout_s = batch_timeout_s

        self._stop_event = threading.Event()
        self._exception: BaseException | None = None
        self._exception_event = threading.Event()

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self.consumer.subscribe(
            [self.input_topic],
            on_revoke=self._on_partitions_revoked,
        )
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.db_conn = psycopg2.connect(db_url)
        self._db_batch: list[tuple] = []
        self._pending_offsets: list = []

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def run(self):
        """Run the processing loop until stop() is called.

        Designed to be started in a background thread, mirroring how a
        production processor runs as a long-lived service. Exceptions are
        captured so callers can detect failures via check_health().
        """
        try:
            self._run_loop()
        except Exception as exc:
            self._exception = exc
            self._exception_event.set()

    def _run_loop(self):
        last_flush = time.time()

        while not self._stop_event.is_set():
            msg = self.consumer.poll(timeout=0.5)
            if msg is None:
                # Flush any pending DB batch on idle timeout
                if self._db_batch:
                    now = time.time()
                    if now - last_flush >= self.batch_timeout_s:
                        self._flush_and_commit()
                        last_flush = now
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(f"Consumer error: {msg.error()}")

            self._handle_message(msg)
            self._pending_offsets.append(msg)

            # Flush DB batch and commit offsets when batch is full
            if len(self._db_batch) >= self.batch_size:
                self._flush_and_commit()
                last_flush = time.time()

        # Final flush on shutdown
        self._flush_and_commit()
        self.producer.flush()

    def stop(self):
        """Signal the processing loop to stop (thread-safe via Event).

        Calls consumer.wakeup() to interrupt a blocking poll() call so the
        loop exits promptly instead of waiting for the poll timeout.
        """
        self._stop_event.set()
        try:
            self.consumer.wakeup()
        except Exception:
            pass  # Consumer may already be closed

    def start_in_background(self) -> threading.Thread:
        """Start the processor in a daemon thread. Returns the thread."""
        thread = threading.Thread(target=self.run, daemon=True)
        thread.start()
        return thread

    def check_health(self):
        """Raise if the processor thread encountered an exception.

        Call this from tests after each test to detect silent thread
        failures instead of waiting for a timeout.
        """
        if self._exception_event.is_set():
            raise RuntimeError(
                f"Processor thread failed: {self._exception}"
            ) from self._exception

    def close(self):
        self._flush_and_commit()
        self.consumer.close()
        self.producer.flush()
        self.db_conn.close()

    # ------------------------------------------------------------------
    # Flush and commit (offset committed AFTER DB flush)
    # ------------------------------------------------------------------

    def _flush_and_commit(self):
        """Flush the DB batch, then commit consumer offsets.

        This ordering provides at-least-once semantics: if we crash after
        the DB flush but before the offset commit, the message will be
        redelivered and reprocessed. The alert_id primary key provides
        natural deduplication on replay.
        """
        self._flush_db_batch()

        if self._pending_offsets:
            # Commit the offset of the last processed message
            self.consumer.commit(
                message=self._pending_offsets[-1], asynchronous=False
            )
            self._pending_offsets.clear()

    # ------------------------------------------------------------------
    # Rebalance handling
    # ------------------------------------------------------------------

    def _on_partitions_revoked(self, consumer, partitions):
        """Called before partitions are revoked during a consumer group rebalance.

        Flushes any pending DB batch and commits offsets so no in-flight
        work is lost when partitions move to another consumer. Without this
        callback, the commit-after-DB-flush ordering cannot guarantee
        at-least-once during rebalance events.
        """
        self._flush_and_commit()

    # ------------------------------------------------------------------
    # Message handling
    # ------------------------------------------------------------------

    def _get_incoming_headers(self, msg) -> list[tuple[str, bytes]]:
        return list(msg.headers()) if msg.headers() else []

    def _get_correlation_id(self, headers: list[tuple[str, bytes]]) -> str | None:
        for key, value in headers:
            if key == "correlation-id":
                return value.decode("utf-8")
        return None

    def _handle_message(self, msg):
        incoming_headers = self._get_incoming_headers(msg)

        try:
            event = TelemetryEvent.model_validate_json(msg.value())
        except (ValidationError, json.JSONDecodeError) as exc:
            self._send_to_dead_letter(msg, str(exc), incoming_headers)
            return

        if event.value >= self.threshold:
            correlation_id = self._get_correlation_id(incoming_headers)
            alert = self._create_alert(event)
            self._produce_alert(alert, incoming_headers)
            self._queue_db_write(alert, correlation_id)

    def _create_alert(self, event: TelemetryEvent) -> Alert:
        severity = "critical" if event.value >= self.threshold + 20 else "high"
        return Alert(
            alert_id=str(uuid.uuid4()),
            source_id=event.source_id,
            metric_name=event.metric_name,
            value=event.value,
            threshold=self.threshold,
            severity=severity,
            triggered_at=datetime.now(timezone.utc),
        )

    def _produce_alert(self, alert: Alert, incoming_headers: list[tuple[str, bytes]]):
        self.producer.produce(
            self.output_topic,
            key=alert.source_id.encode(),
            value=alert.model_dump_json().encode(),
            headers=incoming_headers,
        )
        self.producer.flush()

    def _queue_db_write(self, alert: Alert, correlation_id: str | None = None):
        self._db_batch.append((
            alert.alert_id, alert.source_id, alert.metric_name,
            alert.value, alert.threshold, alert.severity, alert.triggered_at,
            correlation_id,
        ))

    def _flush_db_batch(self):
        if not self._db_batch:
            return
        cursor = self.db_conn.cursor()
        cursor.executemany(
            """INSERT INTO alerts
               (alert_id, source_id, metric_name, value, threshold, severity, triggered_at, correlation_id)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (alert_id) DO NOTHING""",
            self._db_batch,
        )
        self.db_conn.commit()
        cursor.close()
        self._db_batch.clear()

    def _send_to_dead_letter(self, msg, error_reason: str, incoming_headers: list[tuple[str, bytes]]):
        headers = list(incoming_headers) + [
            ("error_reason", f"deserialization: {error_reason}".encode()),
            ("original_topic", (msg.topic() or "unknown").encode()),
            ("failed_at", datetime.now(timezone.utc).isoformat().encode()),
        ]
        self.producer.produce(
            self.dlt_topic,
            value=msg.value(),
            headers=headers,
        )
        self.producer.flush()
