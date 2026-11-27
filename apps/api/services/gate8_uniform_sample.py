"""A uniform random Gate-8 sample over every row a streaming run offered the writer (P0-5).

Gate-8's value sample used to be the first rows the run read (then the lowest
keys among them): a sample that says nothing about the rows it did not check,
so the Trust Report could never state the 95% upper bound the docs promised,
and every sampled run was unsignable.

This module draws a **uniform random sample without replacement** of up to
``k`` rows from the whole population the run streamed, in one pass and in
memory bounded by ``k`` per page:

* each page (``idx``) draws ``min(k, n_page)`` row positions uniformly with a
  generator seeded by ``"<seed>:<idx>"``;
* the coordinator merges each page's sample into the running sample in commit
  order: of ``min(k, N + n)`` slots, the number taken from the page follows the
  hypergeometric law (drawn as ``rng.sample(range(N + n), ...)``, seeded by
  ``"<seed>:merge:<idx>"``), and the slots are filled by uniform sub-samples of
  the running sample and of the page's sample. Merging two uniform samples
  this way gives a uniform sample of the union (Algorithm "merge of
  reservoirs"; see ``tests/test_gate8_uniform_sample.py`` for the frequency
  test).

The seed is random per run (``secrets``) and recorded with the sample
(``reconcile_sample_draw``), so the draw is reproducible for an auditor who
replays the same pages, and the sampled keys are listed in the Gate-8 report.
The Clopper-Pearson bound in :mod:`services.parity_stats` is valid for this
method only (:data:`services.parity_evidence.UNIFORM_RANDOM_SAMPLE_METHODS`).
"""

from __future__ import annotations

import logging
import random
import secrets
from typing import Any

logger = logging.getLogger(__name__)

METHOD = "uniform_random"
ALGORITHM = (
    "uniform random sample without replacement over every row the run offered the writer: "
    "a seeded draw per page, merged across pages with hypergeometric slot counts"
)


def new_draw(size: int) -> dict[str, Any]:
    """The draw's identity for one run: method, a fresh random seed, the sample size."""
    return {"method": METHOD, "seed": secrets.token_hex(16), "size": int(size), "algorithm": ALGORITHM}


def page_sample_indices(seed: str, idx: int, n: int, k: int) -> list[int]:
    """Positions of a uniform sample of ``min(k, n)`` rows of page ``idx`` (``n`` rows)."""
    if n <= 0 or k <= 0:
        return []
    rng = random.Random(f"{seed}:{int(idx)}")  # nosec B311 - sampling, not security
    return sorted(rng.sample(range(n), min(k, n)))


class UniformSampleMerger:
    """The running uniform sample over every page merged so far."""

    def __init__(self, seed: str, k: int) -> None:
        self.seed = str(seed)
        self.k = max(int(k), 0)
        self.population = 0
        self.rows: list[Any] = []
        #: True when this run resumed without the earlier pages' draw: the
        #: sample then covers only the rows written after the resume (T3).
        self.partial = False

    def add(self, idx: int, page_sample: list[Any], page_rows: int) -> None:
        """Merge page ``idx``'s uniform sample (of ``page_rows`` rows) into the running one."""
        n = max(int(page_rows), 0)
        if n == 0:
            return
        page_sample = list(page_sample)[: min(self.k, n)]
        total = self.population + n
        size = min(self.k, total)
        rng = random.Random(f"{self.seed}:merge:{int(idx)}")  # nosec B311 - sampling, not security
        slots = rng.sample(range(total), size)
        from_page = sum(1 for s in slots if s >= self.population)
        # The page's sample holds min(k, n) >= from_page rows; the running one min(k, N) >= the rest.
        from_page = min(from_page, len(page_sample))
        keep = min(size - from_page, len(self.rows))
        self.rows = rng.sample(self.rows, keep) + rng.sample(page_sample, from_page)
        self.population = total

    def draw_record(self, draw: dict[str, Any]) -> dict[str, Any]:
        """What Gate-8 and the Trust Report need to know about this draw."""
        return {
            **draw, "population_rows": self.population, "drawn_rows": len(self.rows),
            "covers_whole_run": not self.partial,
        }

    def state(self, draw: dict[str, Any], chunk_index: int) -> dict[str, Any]:
        """The checkpointed draw after page ``chunk_index``: enough to continue it.

        The drawn rows are source values: at rest in the job store they are
        sealed with the platform key (``sealed_rows``, review 3 item 6), never
        stored in the clear. A seal that fails drops them and marks the state
        partial, so a resume states no bound rather than a wrong one.
        """
        out = {
            **draw, "population_rows": self.population,
            "chunk_index": int(chunk_index), "partial": self.partial,
        }
        try:
            out["sealed_rows"] = seal_rows(self.rows)
        except Exception:  # noqa: BLE001 - no sealed rows: the resumed draw is partial
            logger.warning("Gate-8 draw rows could not be sealed; a resume will be partial",
                           exc_info=True)
            out["partial"] = True
        return out


# -- sealing the drawn rows at rest ------------------------------------------------


def _encode(value: Any) -> Any:
    import base64
    from datetime import date, datetime, time, timedelta
    from decimal import Decimal
    from uuid import UUID

    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return {"$t": "dec", "v": str(value)}
    if isinstance(value, datetime):
        return {"$t": "dt", "v": value.isoformat()}
    if isinstance(value, date):
        return {"$t": "d", "v": value.isoformat()}
    if isinstance(value, time):
        return {"$t": "tm", "v": value.isoformat()}
    if isinstance(value, timedelta):
        return {"$t": "td", "v": [value.days, value.seconds, value.microseconds]}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$t": "b", "v": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, UUID):
        return {"$t": "u", "v": str(value)}
    if isinstance(value, dict):
        return {"$t": "m", "v": [[str(k), _encode(v)] for k, v in value.items()]}
    if isinstance(value, (list, tuple)):
        return {"$t": "l", "v": [_encode(v) for v in value]}
    return {"$t": "s", "v": str(value)}


def _decode(value: Any) -> Any:
    import base64
    from datetime import date, datetime, time, timedelta
    from decimal import Decimal
    from uuid import UUID

    if not isinstance(value, dict):
        return value
    tag, inner = value.get("$t"), value.get("v")
    if tag == "dec":
        return Decimal(inner)
    if tag == "dt":
        return datetime.fromisoformat(inner)
    if tag == "d":
        return date.fromisoformat(inner)
    if tag == "tm":
        return time.fromisoformat(inner)
    if tag == "td":
        return timedelta(days=inner[0], seconds=inner[1], microseconds=inner[2])
    if tag == "b":
        return base64.b64decode(inner)
    if tag == "u":
        return UUID(inner)
    if tag == "m":
        return {k: _decode(v) for k, v in inner}
    if tag == "l":
        return [_decode(v) for v in inner]
    return inner


def seal_rows(rows: list[Any]) -> str:
    """The drawn rows, typed and encrypted with the platform key."""
    import json

    from services.secret_vault import _fernet_encrypt

    text = json.dumps([_encode(row) for row in rows if isinstance(row, dict)], separators=(",", ":"))
    return _fernet_encrypt(text)


def open_rows(sealed: str) -> list[dict[str, Any]]:
    """The rows :func:`seal_rows` sealed."""
    import json

    from services.secret_vault import _fernet_decrypt

    rows = [_decode(item) for item in json.loads(_fernet_decrypt(sealed))]
    return [row for row in rows if isinstance(row, dict)]


def _state_rows(state: dict[str, Any]) -> list[dict[str, Any]] | None:
    """The checkpointed drawn rows (sealed, or a pre-seal checkpoint's clear rows)."""
    sealed = state.get("sealed_rows")
    if isinstance(sealed, str) and sealed:
        try:
            return open_rows(sealed)
        except Exception:  # noqa: BLE001 - unreadable: the resumed draw is partial
            logger.warning("Gate-8 draw rows could not be opened; the resumed draw is partial",
                           exc_info=True)
            return None
    return [row for row in state.get("rows") or [] if isinstance(row, dict)]


def resume_draw(checkpoint: Any, k: int, *, resumed: bool) -> tuple[dict[str, Any], UniformSampleMerger]:
    """``(draw, merger)`` for this pass: the checkpoint's draw when resuming (T3).

    A resumed pass continues the seed, population and rows the checkpoint
    carried, so the sample is still uniform over every row the run offered
    the writer. A resumed pass without that state starts a draw over the new
    pages only and is marked ``partial``: it is not reported as uniform over
    the run, and no bound is stated for it.
    """
    state = getattr(checkpoint, "reconcile_sample_draw_state", None) if resumed else None
    if (
        isinstance(state, dict) and state.get("seed") and state.get("method") == METHOD
        and not state.get("partial")
        # Saved with the same page as the resume point: an older copy (the
        # job document lagging the sidecar token) would miss pages.
        and state.get("chunk_index") is not None
        and int(state["chunk_index"]) == int(getattr(checkpoint, "chunk_index", 0) or 0)
    ):
        draw = {key: state[key] for key in ("method", "seed", "size", "algorithm") if key in state}
        merger = UniformSampleMerger(str(state["seed"]), int(state.get("size") or k))
        rows = _state_rows(state)
        if rows is not None:
            merger.population = max(int(state.get("population_rows") or 0), 0)
            merger.rows = rows[: merger.k]
            return draw, merger
    draw = new_draw(k)
    merger = UniformSampleMerger(draw["seed"], k)
    merger.partial = bool(resumed)
    return draw, merger
