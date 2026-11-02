"""Policy-aware verification sample and checksum survivors.

Upsert lands the last image of a key. skip_duplicates lands the first. The
read-back sample and the source digest have to be that same image, or a
correct load fails Gate-8 (first amount versus the last amount on the table).
Append keeps every row: duplicate keys are the data.
"""

from __future__ import annotations

from typing import Any

_SAMPLE_LIMIT = 50


def _policy_keeps_first(policy: str) -> bool | None:
    """True for skip, False for upsert, None when the policy does not collapse."""
    from services.duplicate_policy import normalize_duplicate_policy

    token = normalize_duplicate_policy(policy)
    if token == "dedupe_skip":
        return True
    if token == "upsert":
        return False
    return None


def _identity(row: Any, key_cols: list[str], columns: list[str] | None = None) -> tuple | None:
    if not key_cols:
        return None
    if isinstance(row, dict):
        values = tuple(row.get(col) for col in key_cols)
    else:
        if not columns:
            return None
        values = []
        for col in key_cols:
            if col not in columns:
                return None
            index = columns.index(col)
            values.append(row[index] if index < len(row) else None)
        values = tuple(values)
    if any(value in (None, "") for value in values):
        return None
    return values


class PolicySampleBook:
    """First 50 source keys, with the image the write will keep."""

    def __init__(self, key_fields: list[str], policy: str) -> None:
        self.key_fields = [str(col) for col in key_fields if str(col).strip()]
        self.keep_first = _policy_keeps_first(policy)
        self.rows: dict[tuple, dict] = {}
        self.order: list[tuple] = []
        self.loose: list[dict] = []

    @property
    def active(self) -> bool:
        return self.keep_first is not None and bool(self.key_fields)

    def add_batch(self, batch: list[dict] | None, *, limit: int | None = _SAMPLE_LIMIT) -> None:
        if not self.active or not batch:
            return
        for rec in batch:
            if not isinstance(rec, dict):
                continue
            identity = _identity(rec, self.key_fields)
            if identity is None:
                # Null keys are not collapsed. The read-back sample only
                # keeps fifty of them, so a dirty file does not retain every
                # rejected row just to fill that window.
                loose_cap = _SAMPLE_LIMIT if limit is None else limit
                if len(self.loose) < loose_cap:
                    self.loose.append(dict(rec))
                continue
            seen = identity in self.rows
            if seen and self.keep_first:
                continue
            if seen:
                self.rows[identity] = dict(rec)
                continue
            if limit is not None and len(self.order) >= limit:
                continue
            self.rows[identity] = dict(rec)
            self.order.append(identity)

    def export(self) -> list[dict]:
        """Every survivor this book holds, including keys past the sample cap.

        A chunk collapses its own duplicates before the parent applies the
        fifty-row cap. The later image of a key the parent already admitted
        has to travel with the chunk or the sample keeps the first amount.
        """
        rows = [self.rows[key] for key in self.order]
        rows.extend(self.loose)
        return rows

    def sample(self) -> list[dict]:
        rows = [self.rows[key] for key in self.order]
        room = _SAMPLE_LIMIT - len(rows)
        if room > 0:
            rows.extend(self.loose[:room])
        return rows


class PolicyFingerprintBook:
    """Survivor fingerprints across batches. Append stores every fingerprint."""

    def __init__(self, policy: str, key_cols: list[str]) -> None:
        self.key_cols = [str(col) for col in key_cols if str(col).strip()]
        self.keep_first = _policy_keeps_first(policy)
        self.survivors: dict[tuple, tuple[str, str]] = {}
        self.loose: list[tuple[str, str]] = []
        self.dropped = 0
        self._flushed = False

    @property
    def active(self) -> bool:
        return self.keep_first is not None and bool(self.key_cols)

    def add_aligned(
        self,
        identities: list[tuple | None],
        fingerprints: list[tuple[str, str]],
    ) -> None:
        if not fingerprints:
            return
        if not self.active:
            self.loose.extend(fingerprints)
            return
        for identity, fingerprint in zip(identities, fingerprints):
            if identity is None:
                self.loose.append(fingerprint)
                continue
            if identity in self.survivors:
                self.dropped += 1
                if not self.keep_first:
                    self.survivors[identity] = fingerprint
                continue
            self.survivors[identity] = fingerprint
        extra = len(fingerprints) - len(identities)
        if extra > 0:
            self.loose.extend(fingerprints[len(identities) :])

    def add(
        self,
        mapped_rows: list[Any],
        fingerprints: list[tuple[str, str]],
        *,
        columns: list[str],
    ) -> None:
        identities = [_identity(row, self.key_cols, columns) for row in mapped_rows]
        self.add_aligned(identities, fingerprints)

    def flush(self, accumulator: Any) -> None:
        if self._flushed:
            return
        self._flushed = True
        if self.survivors:
            accumulator.add_many(self.survivors.values())
        if self.loose:
            accumulator.add_many(self.loose)
