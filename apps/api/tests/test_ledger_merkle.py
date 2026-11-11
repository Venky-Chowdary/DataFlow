"""The RFC 6962 Merkle tree behind the Evidence Ledger and the exit rehearsal seal.

Pins: the Certificate Transparency known-answer vectors (roots, an audit path,
a consistency proof); for any tree, every leaf has a valid inclusion proof at
every later size and any two sizes have a valid consistency proof; a changed
leaf, root, proof hash, index or size fails; the exit seal still computes the
root it always did (one tree implementation).
"""

from __future__ import annotations

import hashlib

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from services.ledger_merkle import (
    EMPTY_ROOT,
    Tree,
    consistency_holds,
    leaf_hash,
    node_hash,
    root_from_inclusion,
    root_of,
)

# Certificate Transparency reference leaves and tree heads (RFC 6962 test vectors).
CT_LEAVES = ["", "00", "10", "2021", "3031", "40414243", "5051525354555657",
             "606162636465666768696a6b6c6d6e6f"]
CT_ROOTS = [
    "6e340b9cffb37a989ca544e6bb780a2c78901d3fb33738768511a30617afa01d",
    "fac54203e7cc696cf0dfcb42c92a1d9dbaf70ad9e621f4bd8d98662f00e3c125",
    "aeb6bcfe274b70a14fb067a5e5578264db0fa9b51af5e0ba159158f329e06e77",
    "d37ee418976dd95753c1c73862b9398fa2a2cf9b4ff0fdfe8b30cd95209614b7",
    "4e3bbb1f7b478dcfe71fb631631519a3bca12c9aefca1612bfce4c13a86264d4",
    "76e67dadbcdf1e10e1b74ddc608abd2f98dfb16fbce75277b5232a127f2087ef",
    "ddb89be403809e325750d3d263cd78929c2942b7942a34b77e122c9594a74c8c",
    "5dc9da79a70659a9ad559cb701ded9a2ab9d823aad2f4960cfe370eff4604328",
]


def _ct_tree() -> Tree:
    return Tree(leaf_hash(bytes.fromhex(x)) for x in CT_LEAVES)


def _naive_root(leaves: list[bytes]) -> bytes:
    """RFC 6962 2.1, written out directly (the definition the tree is checked against)."""
    if not leaves:
        return hashlib.sha256(b"").digest()
    if len(leaves) == 1:
        return leaves[0]
    k = 1
    while k * 2 < len(leaves):
        k *= 2
    return hashlib.sha256(b"\x01" + _naive_root(leaves[:k]) + _naive_root(leaves[k:])).digest()


def test_known_answer_roots_and_domain_separated_prefixes():
    tree = _ct_tree()
    assert tree.root(0) == EMPTY_ROOT == hashlib.sha256(b"").digest()
    assert [tree.root(n).hex() for n in range(1, 9)] == CT_ROOTS
    assert leaf_hash(b"") == hashlib.sha256(b"\x00").digest()
    assert node_hash(b"a" * 32, b"b" * 32) == hashlib.sha256(b"\x01" + b"a" * 32 + b"b" * 32).digest()
    # A leaf can never be read as an inner node: the prefixes differ.
    assert leaf_hash(b"a" * 32 + b"b" * 32) != node_hash(b"a" * 32, b"b" * 32)
    assert root_of(bytes.fromhex(x) for x in CT_LEAVES).hex() == CT_ROOTS[-1]


def test_known_answer_inclusion_and_consistency_proofs():
    tree = _ct_tree()
    assert [h.hex() for h in tree.inclusion_proof(0, 8)] == [
        "96a296d224f285c67bee93c30f8a309157f0daa35dc5b87e410b78630a09cfc7",
        "5f083f0a1a33ca076a95279832580db3e0ef4584bdff1f54c8a360f50de3031e",
        "6b47aaf29ee3c2af9af889bc1fb9254dabd31177f16232dd6aab035ca39bf6e4",
    ]
    assert [h.hex() for h in tree.consistency_proof(6, 8)] == [
        "0ebc5d3437fbe2db158b9f126a1d118e308181031d0a949f8dededebc558ef6a",
        "ca854ea128ed050b41b35ffc1b87b8eb2bde461e9e3b5596ece6b9d5975a0ae0",
        "d37ee418976dd95753c1c73862b9398fa2a2cf9b4ff0fdfe8b30cd95209614b7",
    ]
    assert [h.hex() for h in tree.consistency_proof(4, 8)] == [
        "6b47aaf29ee3c2af9af889bc1fb9254dabd31177f16232dd6aab035ca39bf6e4"]
    assert tree.consistency_proof(8, 8) == [] and tree.consistency_proof(0, 8) == []


leaf_lists = st.lists(st.binary(min_size=0, max_size=40), min_size=1, max_size=70)


@settings(max_examples=60, deadline=None)
@given(leaf_lists)
def test_every_leaf_has_a_valid_inclusion_proof_at_every_later_size(data):
    tree = Tree(leaf_hash(d) for d in data)
    hashes = [leaf_hash(d) for d in data]
    for size in range(1, len(data) + 1):
        root = tree.root(size)
        assert root == _naive_root(hashes[:size])
        for index in range(size):
            proof = tree.inclusion_proof(index, size)
            assert root_from_inclusion(hashes[index], index, size, proof) == root


@settings(max_examples=60, deadline=None)
@given(leaf_lists)
def test_consistency_holds_between_any_two_sizes(data):
    tree = Tree(leaf_hash(d) for d in data)
    for second in range(1, len(data) + 1):
        for first in range(0, second + 1):
            proof = tree.consistency_proof(first, second)
            assert consistency_holds(first, second, tree.root(first), tree.root(second), proof)


@settings(max_examples=40, deadline=None)
@given(leaf_lists, st.data())
def test_a_tampered_leaf_root_or_proof_fails(data, draw):
    tree = Tree(leaf_hash(d) for d in data)
    size = draw.draw(st.integers(min_value=1, max_value=len(data)))
    index = draw.draw(st.integers(min_value=0, max_value=size - 1))
    root, proof, leaf = tree.root(size), tree.inclusion_proof(index, size), tree.leaf(index)

    def leads_to_root(leaf_, index_, size_, proof_):
        try:
            return root_from_inclusion(leaf_, index_, size_, proof_) == root
        except ValueError:
            return False

    assert leads_to_root(leaf, index, size, proof)
    flipped = bytes([leaf[0] ^ 1]) + leaf[1:]
    assert not leads_to_root(flipped, index, size, proof)
    for i in range(len(proof)):
        bad = list(proof)
        bad[i] = bytes([bad[i][0] ^ 1]) + bad[i][1:]
        assert not leads_to_root(leaf, index, size, bad)
    assert not leads_to_root(leaf, index, size, proof + [b"\x00" * 32])
    if proof:
        assert not leads_to_root(leaf, index, size, proof[:-1])
    if size > 1:
        other = (index + 1) % size
        if tree.leaf(other) != leaf:
            assert not leads_to_root(tree.leaf(other), index, size, proof)

    first = draw.draw(st.integers(min_value=1, max_value=size))
    cproof = tree.consistency_proof(first, size)
    wrong_root = bytes([root[0] ^ 1]) + root[1:]
    assert not consistency_holds(first, size, tree.root(first), wrong_root, cproof)
    wrong_first = bytes([tree.root(first)[0] ^ 1]) + tree.root(first)[1:]
    assert not consistency_holds(first, size, wrong_first, root, cproof)
    for i in range(len(cproof)):
        bad = list(cproof)
        bad[i] = bytes([bad[i][0] ^ 1]) + bad[i][1:]
        assert not consistency_holds(first, size, tree.root(first), root, bad)
    assert not consistency_holds(size + 1, size, tree.root(first), root, cproof)


@settings(max_examples=40, deadline=None)
@given(leaf_lists, st.data())
def test_a_rewritten_log_is_not_consistent_with_an_older_tree_head(data, draw):
    tree = Tree(leaf_hash(d) for d in data)
    held = draw.draw(st.integers(min_value=1, max_value=len(data)))
    changed = draw.draw(st.integers(min_value=0, max_value=held - 1))
    rewritten = [leaf_hash(d) for d in data]
    rewritten[changed] = leaf_hash(data[changed] + b"!")
    forged = Tree(rewritten + [leaf_hash(b"later entry")])
    proof = forged.consistency_proof(held, len(forged))
    # The forger's proof is sound for the forged history ...
    assert consistency_holds(held, len(forged), forged.root(held), forged.root(), proof)
    # ... and cannot be made to fit the tree head a client saved before the rewrite.
    assert not consistency_holds(held, len(forged), tree.root(held), forged.root(), proof)


def test_extending_a_tree_keeps_earlier_roots_and_proofs():
    tree = Tree(leaf_hash(bytes([i])) for i in range(5))
    root5, proof = tree.root(5), tree.inclusion_proof(2, 5)
    tree.extend(leaf_hash(bytes([i])) for i in range(5, 23))
    assert tree.root(5) == root5 and tree.inclusion_proof(2, 5) == proof
    assert len(tree) == 23
    with pytest.raises(ValueError):
        tree.root(24)
    with pytest.raises(ValueError):
        tree.inclusion_proof(23, 23)
    with pytest.raises(ValueError):
        tree.consistency_proof(24, 23)


def test_the_exit_seal_uses_this_tree_and_keeps_its_root():
    from services.exit_completeness import merkle_leaf, merkle_root

    parts = [{"table": "orders", "size_bytes": 10, "sha256": "a" * 64},
             {"table": "events", "size_bytes": 7, "sha256": "b" * 64},
             {"table": "accounts", "size_bytes": 3, "sha256": "c" * 64}]
    ordered = sorted(parts, key=lambda p: p["table"])
    expected = _naive_root([hashlib.sha256(b"\x00" + merkle_leaf(p)).digest() for p in ordered])
    assert merkle_root(parts) == "sha256:" + expected.hex()
    assert merkle_root([]) == "sha256:" + hashlib.sha256(b"").hexdigest()
