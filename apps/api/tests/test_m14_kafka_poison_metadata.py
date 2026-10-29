"""M14: Kafka poison messages are quarantined with broker coordinates instead
of landing as all-NULL rows; tombstones are counted, not silently skipped;
key/partition/offset are exposed as optional metadata columns."""

from connectors.kafka_debezium_bridge import kafka_value_to_row
from connectors import kafka_reader


def test_poison_value_carries_marker_and_broker_coordinates():
    row = kafka_value_to_row(
        "{not json at all",
        meta={"topic": "t", "partition": 2, "offset": 41, "key": "k1"},
    )
    assert row["_kafka_poison"] == "non-JSON Kafka message"
    assert row["_kafka_value"] == "{not json at all"
    assert row["_kafka_partition"] == 2
    assert row["_kafka_offset"] == 41
    assert row["_kafka_key"] == "k1"
    assert row["_kafka_topic"] == "t"


def test_valid_json_row_still_decodes_and_carries_coordinates():
    row = kafka_value_to_row(
        '{"id": 7, "v": "ok"}',
        meta={"topic": "t", "partition": 0, "offset": 9, "key": None},
    )
    assert row == {
        "id": 7,
        "v": "ok",
        "_kafka_topic": "t",
        "_kafka_partition": 0,
        "_kafka_offset": 9,
        # D151: an unkeyed message still carries the key column, as NULL.
        "_kafka_key": None,
    }
    assert "_kafka_poison" not in row


class _Msg:
    def __init__(self, topic, partition, offset, value, key=None):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.value = value
        self.key = key


class _FakeConsumer:
    def __init__(self, cfg):
        self.config = cfg
        self.tombstones_seen = 0
        self._pending = {}
        self._polled = False

    def poll_rows(self, *, max_records=500, timeout_ms=1000):
        if self._polled:
            return
        self._polled = True
        msgs = [
            _Msg("t", 0, 10, b'{"id": 1, "v": "ok"}', key=b"a"),
            _Msg("t", 0, 11, b"{not json at all"),
            _Msg("t", 0, 12, b""),  # tombstone
        ]
        for m in msgs:
            self._pending[(m.topic, m.partition)] = m.offset + 1
            if not m.value:
                self.tombstones_seen += 1
            row = kafka_value_to_row(
                m.value,
                meta={
                    "topic": m.topic,
                    "partition": m.partition,
                    "offset": m.offset,
                    "key": m.key.decode("utf-8", "replace") if isinstance(m.key, bytes) else m.key,
                },
            )
            if row:
                yield row

    def pending_offsets(self):
        return dict(self._pending)

    def has_assignment(self):
        return True

    # The reader asks the bridge whether the run reached its frozen end (D151).
    def exhausted(self):
        return False

    def end_offsets_snapshot(self):
        return []

    def close(self):
        pass


def test_read_topic_batch_quarantines_poison_and_counts_tombstones(monkeypatch):
    monkeypatch.setattr(kafka_reader, "KafkaDebeziumConsumer", _FakeConsumer)
    batch, cursor = kafka_reader.read_topic_batch(
        cfg={"bootstrap_servers": "x:9092"},
        topic="t",
        limit=10,
    )
    # Poison did not land as a data row; the good row did, with coordinates.
    assert len(batch.rows) == 1
    good = dict(zip(batch.headers, batch.rows[0]))
    assert good["id"] == "1"
    assert good["_kafka_offset"] == "10"
    assert good["_kafka_key"] == "a"
    rejected = (batch.meta or {}).get("rejected_rows") or []
    assert len(rejected) == 1
    assert "poison" in rejected[0]["reason"]
    assert rejected[0]["row"]["_kafka_offset"] == "11"
    assert (batch.meta or {}).get("kafka_tombstones_seen") == 1
    assert cursor is not None  # offsets still checkpoint through the poison
