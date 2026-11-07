R='/home/user/r14-cancel8/apps/api/'
def edit(p, pairs):
    s=open(R+p).read()
    for old,new in pairs:
        assert s.count(old)==1, (p, old[:70])
        s=s.replace(old,new)
    open(R+p,'w').write(s)

edit('services/audit_chain_keystore.py', [('''def key_history(*, strict: bool = False)''', '''HIGH_WATER_ID = "__high_water__"


def _hw_mac(seq: int, head: str) -> str:
    return _mac("high-water", str(int(seq)), str(head or ""))


def high_water() -> dict[str, Any] | None:
    """The witnessed chain head ``{seq, head, problem}`` (MAC'd, kept twice), or None.

    Kept on the key-history collection and on the keyring document; the
    higher valid copy wins. A copy that fails its MAC is a ``problem``.
    """
    import hmac

    best: dict[str, Any] | None = None
    problem = ""
    copies = []
    hist = _history_coll()
    if hist is not None:
        copies.append(hist.find_one({"_id": HIGH_WATER_ID}))
    keys = _collection()
    if keys is not None:
        copies.append(((keys.find_one({"_id": DOC_ID}, {"high_water": 1}) or {}).get("high_water")))
    for copy in copies:
        if not isinstance(copy, dict):
            continue
        try:
            seq, head = int(copy.get("seq") or 0), str(copy.get("head") or "")
        except (TypeError, ValueError):
            seq, head = 0, ""
        if not hmac.compare_digest(_hw_mac(seq, head), str(copy.get("mac") or "")):
            problem = "the chain's witnessed high-water mark fails its MAC"
            continue
        if best is None or seq > best["seq"]:
            best = {"seq": seq, "head": head}
    if best is None and not problem:
        return None
    return {**(best or {"seq": 0, "head": ""}), "problem": problem}


def witness_head(seq: int, head: str) -> None:
    """Raise the witnessed head to ``(seq, head)`` in both places (never lowers it)."""
    from pymongo.errors import DuplicateKeyError

    if not seq or not head:
        return
    body = {"seq": int(seq), "head": str(head), "mac": _hw_mac(seq, head), "at": _now()}
    below = {"$or": [{"seq": {"$lt": int(seq)}}, {"seq": {"$exists": False}}]}
    hist = _history_coll()
    if hist is not None:
        try:
            hist.update_one({"_id": HIGH_WATER_ID, **below}, {"$set": body}, upsert=True)
        except DuplicateKeyError:
            pass  # a higher head is already witnessed
    keys = _collection()
    if keys is not None:
        keys.update_one(
            {"_id": DOC_ID, "$or": [{"high_water.seq": {"$lt": int(seq)}}, {"high_water": {"$exists": False}}]},
            {"$set": {"high_water": body}},
        )


def key_history(*, strict: bool = False)'''),
  ('''        for doc in coll.find({"_id": {"$ne": _EPOCH_ID}}):
            entry = _valid(doc)''', '''        for doc in coll.find({"_id": {"$nin": [_EPOCH_ID, HIGH_WATER_ID]}}):
            entry = _valid(doc)'''),
])

edit('services/audit_log.py', [('''def _after_append(persisted: dict[str, Any]) -> None:
    """Keyring seal records and the chain's high-water mark, outside the append lock."""
    try:
        from services import audit_chain_keystore as store

        store.flush_seal_record()
    except Exception:  # noqa: BLE001 - bookkeeping never fails the write
        logging.getLogger(__name__).debug("audit keyring seal not flushed", exc_info=True)''', '''#: The chain's witnessed head (tail-truncation evidence) is raised every this many writes.
HIGH_WATER_EVERY = 25
_writes_since_witness = [0]


def _after_append(persisted: dict[str, Any]) -> None:
    """Keyring seal records and the chain's high-water mark, outside the append lock."""
    try:
        from services import audit_chain_keystore as store

        store.flush_seal_record()
        _writes_since_witness[0] += 1
        if _writes_since_witness[0] >= HIGH_WATER_EVERY and _mongo_collection() is not None:
            _writes_since_witness[0] = 0
            store.witness_head(int(persisted.get("chain_seq") or 0), str(persisted.get("event_hash") or ""))
    except Exception:  # noqa: BLE001 - bookkeeping never fails the write
        logging.getLogger(__name__).debug("audit keyring seal / high-water not written", exc_info=True)'''),
])

edit('services/evidence_chain.py', [
  ('''    history_tampered = anchor["tampered"]
    keyring_tampered = anchor["keyring_tampered"]''', '''    history_tampered = anchor["tampered"]
    keyring_tampered = anchor["keyring_tampered"]
    tail = _tail_truncation()'''),
  ('''    if keyring_tampered:
        categories["tampering"] += 1
    if anchor["secrets_key_changed"]:''', '''    if keyring_tampered:
        categories["tampering"] += 1
    if tail:
        categories["tampering"] += 1
    if anchor["secrets_key_changed"]:'''),
  ('''            "verified": (not found and not history_tampered and not keyring_tampered and not prior
                         and not anchor["secrets_key_changed"]),''', '''            "verified": (not found and not history_tampered and not keyring_tampered and not prior
                         and not anchor["secrets_key_changed"] and not tail),
            "tail_truncated": tail,'''),
  ('''    honesty = (
        "Verification covers the records still in the store: it proves they "
        "were not edited or removed since a holder of the chain key they "
        "name wrote them. It does not prove the recorded facts are true, nor that "
        "records were never discarded together with their checkpoint. Only an "
        "external WORM / timestamp anchor narrows that."
    )''', '''    honesty = (
        "Verification proves the records in the store were not edited, and that "
        "none was removed from the middle of the chain, since a holder of the "
        "chain key they name wrote them. Removal of the NEWEST records is detected "
        "only up to the last witnessed head (a MAC'd high-water mark raised every "
        "few writes and by every verify); closing that fully needs an external "
        "WORM or timestamp anchor. It does not prove the recorded facts are true, "
        "nor that records were never discarded together with their checkpoint."
    )'''),
  ('''def _secrets_key_changed(''', '''def _tail_truncation() -> dict[str, Any] | None:
    """The store's head against the witnessed high-water mark (HIGH 3).

    ``None`` when the head is at or past it; otherwise ``{witnessed_seq,
    store_head_seq, reason}``. Afterwards the current head is witnessed (the
    mark only ever rises), so every verify extends the evidence.
    """
    from services import audit_chain_keystore as keystore

    try:
        from services.control_plane_store import mongo_collection

        coll = mongo_collection("audit_events")
        if coll is None:
            return None
        hw = keystore.high_water()
        top = next(iter(coll.find({"_id": {"$regex": "^audit-chain:"}}).sort("chain_seq", -1).limit(1)), None)
        head_seq = _position(top or {})
        out: dict[str, Any] | None = None
        if hw and hw.get("problem"):
            out = {"witnessed_seq": hw.get("seq"), "store_head_seq": head_seq, "reason": hw["problem"]}
        elif hw and hw.get("seq"):
            if head_seq < hw["seq"]:
                out = {"witnessed_seq": hw["seq"], "store_head_seq": head_seq,
                       "reason": f"the newest {hw['seq'] - head_seq} record(s) up to the witnessed "
                                 f"head at position {hw['seq']} are gone"}
            else:
                at = coll.find_one({"chain_seq": hw["seq"], "_id": {"$regex": "^audit-chain:"}})
                if at is None or str(at.get("event_hash") or "") != hw["head"]:
                    out = {"witnessed_seq": hw["seq"], "store_head_seq": head_seq,
                           "reason": f"the record at the witnessed head (position {hw['seq']}) "
                                     "was removed or replaced"}
        if out is None and top is not None:
            keystore.witness_head(head_seq, str(top.get("event_hash") or ""))
        return out
    except Exception as exc:  # noqa: BLE001 - no evidence either way
        logger.warning("audit chain high-water not checked: %s", type(exc).__name__)
        return None


def _secrets_key_changed('''),
])
print("item2 applied")
