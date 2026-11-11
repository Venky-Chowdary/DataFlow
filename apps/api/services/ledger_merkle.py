"""The RFC 6962 Merkle tree: roots, inclusion proofs and consistency proofs.

The one Merkle tree in the service. The leaf and node hashes (``SHA-256(0x00 ‖
leaf)``, ``SHA-256(0x01 ‖ left ‖ right)``) and the proof *verification* live in
the standalone verifier (:mod:`tools.datawrap_verify`), which the service
imports, so a proof made here is checked by the same code an auditor runs.
The exit rehearsal seal (:func:`services.exit_completeness.merkle_root`) and
the Evidence Ledger both build their trees here.

:class:`Tree` holds leaf hashes and remembers the hash of every complete,
aligned subtree it has computed. Leaves are append-only, so those never
change: after the first pass a root or a proof costs O(log n) hashes.
"""

from __future__ import annotations

from collections.abc import Iterable

from tools.datawrap_verify import (  # noqa: F401 - re-exported: the tree's public surface
    EMPTY_ROOT,
    consistency_holds,
    leaf_hash,
    node_hash,
    root_from_inclusion,
)


def _split(n: int) -> int:
    """The largest power of two strictly smaller than ``n`` (n > 1)."""
    return 1 << ((n - 1).bit_length() - 1)


class Tree:
    """An append-only list of leaf hashes with RFC 6962 roots and proofs."""

    def __init__(self, leaf_hashes: Iterable[bytes] = ()) -> None:
        self._leaves: list[bytes] = list(leaf_hashes)
        self._memo: dict[tuple[int, int], bytes] = {}

    def __len__(self) -> int:
        return len(self._leaves)

    def extend(self, leaf_hashes: Iterable[bytes]) -> None:
        self._leaves.extend(leaf_hashes)

    def leaf(self, index: int) -> bytes:
        return self._leaves[index]

    def _range(self, lo: int, hi: int) -> bytes:
        """MTH of leaves ``[lo, hi)``."""
        width = hi - lo
        if width == 1:
            return self._leaves[lo]
        aligned = width & (width - 1) == 0 and lo % width == 0
        if aligned and (lo, hi) in self._memo:
            return self._memo[(lo, hi)]
        k = _split(width)
        value = node_hash(self._range(lo, lo + k), self._range(lo + k, hi))
        if aligned:
            self._memo[(lo, hi)] = value
        return value

    def _size(self, size: int | None) -> int:
        n = len(self._leaves) if size is None else int(size)
        if n < 0 or n > len(self._leaves):
            raise ValueError(f"tree size {n} is outside the log (0..{len(self._leaves)})")
        return n

    def root(self, size: int | None = None) -> bytes:
        n = self._size(size)
        return self._range(0, n) if n else EMPTY_ROOT

    def inclusion_proof(self, index: int, size: int | None = None) -> list[bytes]:
        """The audit path for leaf ``index`` in the tree of the first ``size`` leaves."""
        n = self._size(size)
        if index < 0 or index >= n:
            raise ValueError(f"leaf {index} is not in a tree of {n}")
        path: list[bytes] = []
        lo, hi = 0, n
        while hi - lo > 1:
            k = _split(hi - lo)
            if index < lo + k:
                path.append(self._range(lo + k, hi))
                hi = lo + k
            else:
                path.append(self._range(lo, lo + k))
                lo = lo + k
        path.reverse()
        return path

    def consistency_proof(self, first: int, second: int | None = None) -> list[bytes]:
        """Proof that the tree of ``second`` leaves extends the tree of ``first`` leaves."""
        n = self._size(second)
        if first < 0 or first > n:
            raise ValueError(f"size {first} is not within a tree of {n}")
        if first == 0 or first == n:
            return []
        return self._subproof(first, 0, n, True)

    def _subproof(self, m: int, lo: int, hi: int, whole: bool) -> list[bytes]:
        if m == hi - lo:
            return [] if whole else [self._range(lo, hi)]
        k = _split(hi - lo)
        if m <= k:
            return self._subproof(m, lo, lo + k, whole) + [self._range(lo + k, hi)]
        return self._subproof(m - k, lo + k, hi, False) + [self._range(lo, lo + k)]


def root_of(leaf_data: Iterable[bytes]) -> bytes:
    """The Merkle tree hash of raw leaves (each is hashed with the leaf prefix first)."""
    return Tree(leaf_hash(item) for item in leaf_data).root()
