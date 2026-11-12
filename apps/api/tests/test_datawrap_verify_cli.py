"""The offline verifier (tools/datawrap_verify.py) run as a separate process on real packs.

The packs come from the real signing paths (the proof pack export, Parity
Studio's sign-off, the Exit Evidence sign-off); the tool runs with an empty
environment, in isolated mode, from another directory, so it can use nothing
of the service. Pins: it is one file needing only the standard library and
``cryptography``; each check is printed as passed / failed / not checked; a
sound pack never prints "VERIFIED" while a check was not made; tampered
packs, receipts and keys fail or stay unchecked, never pass; a checkpoint
saved earlier exposes a rewritten log; a withdrawal receipt turns the verdict
to "withdrawn"; the service and the tool agree on the same input.
"""

from __future__ import annotations

import ast
import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_exit_evidence_pack import _sign_all, proven  # noqa: F401
from tests.test_exit_rehearsal_e2e import world  # noqa: F401
from tests.test_ledger_log import ledger  # noqa: F401
from tests.test_ledger_packs import _job
from tests.test_parity_review2_f12_f13_seal_and_verify import _signed, store  # noqa: F401
from tools import datawrap_verify as verifier

TOOL = Path(__file__).resolve().parents[1] / "tools" / "datawrap_verify.py"
OFFLINE_CHECKS = ("content digest", "statement", "signatures", "inclusion in the log", "checkpoint signature")


def _run(cwd: Path, *args: str) -> subprocess.CompletedProcess:
    """The tool as an outsider runs it: isolated interpreter, empty environment, another directory."""
    return subprocess.run([sys.executable, "-I", str(TOOL), *args], cwd=cwd, capture_output=True, text=True,
                          env={"PATH": os.environ.get("PATH", "")}, timeout=120, check=False)


def _write(path: Path, doc) -> str:
    path.write_text(doc if isinstance(doc, str) else json.dumps(doc), encoding="utf-8")
    return str(path)


def _lines(out: str) -> dict[str, str]:
    """check label -> 'passed' | 'FAILED' | 'not checked'."""
    rows = {}
    for line in out.splitlines():
        if " ." in line and ": " in line:
            label, rest = line.split(" .", 1)
            rows[label.strip()] = rest.lstrip(". ").split(":", 1)[0]
    return rows


def _jwks(workspace: str) -> dict:
    from services import ledger_keys

    return ledger_keys.jwks([ledger_keys.scope_for(workspace), ledger_keys.LOG_SCOPE])


def _export(job_id: str, workspace: str = "w1") -> dict:
    from services.signed_proof_pack import export_proof_pack_for_job

    return export_proof_pack_for_job(_job(job_id, workspace), actor="ops@example.com")


def test_the_tool_is_one_file_using_only_the_standard_library_and_cryptography():
    tree = ast.parse(TOOL.read_text(encoding="utf-8"))
    roots = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            roots |= {alias.name.split(".")[0] for alias in node.names}
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "no relative imports: the file stands alone"
            roots.add(str(node.module).split(".")[0])
    assert roots - set(sys.stdlib_module_names) == {"cryptography"}
    helped = _run(TOOL.parent.parent, "--help")
    assert helped.returncode == 0 and "--keys" in helped.stdout and "--previous-checkpoint" in helped.stdout


def test_a_sound_proof_pack_passes_every_offline_check_and_is_not_called_verified(ledger, tmp_path):  # noqa: F811
    pack = _export("job-cli")
    pack_file = _write(tmp_path / "pack.json", pack)
    keys_file = _write(tmp_path / "keys.json", _jwks("w1"))
    done = _run(tmp_path, pack_file, "--keys", keys_file)
    rows = _lines(done.stdout)
    for check in OFFLINE_CHECKS:
        assert rows[check] == "passed", (check, done.stdout)
    assert rows["consistency"] == rows["withdrawal"] == rows["anchor"] == "not checked"
    assert "result: INCOMPLETE (5 passed, 0 failed, 3 not checked)" in done.stdout
    assert "signature scheme: " + verifier.SCHEME_LABELS[
        "hybrid" if verifier.ml_dsa_available() else "classical"] in done.stdout
    assert "VERIFIED" not in done.stdout and done.returncode == 2, done.stdout
    assert "It does not show that the recorded facts are true." in done.stdout
    assert done.stderr == ""
    # --json is the EvidenceVerdict, and equals what the same code answers in process.
    as_json = json.loads(_run(tmp_path, pack_file, "--keys", keys_file, "--json").stdout)
    keys = verifier.KeySet()
    keys.add_text(json.dumps(_jwks("w1")))
    assert as_json == verifier.verify_evidence(pack, None, keys)
    assert as_json["verdict"] == "incomplete" and as_json["mode"] == "offline"
    assert as_json["signature_scheme"] == ("hybrid" if verifier.ml_dsa_available() else "classical")
    # The service agrees on every check both can make.
    from services.ledger_verify import verify_online

    online = {c["id"]: c["status"] for c in verify_online(pack)["checks"]}
    offline = {c["id"]: c["status"] for c in as_json["checks"]}
    for cid in ("content_digest", "statement", "signatures", "inclusion", "checkpoint_signature"):
        assert online[cid] == offline[cid] == "passed"


def test_the_receipt_may_be_a_separate_file_and_keys_may_be_pem(ledger, tmp_path):  # noqa: F811
    from services import ledger_keys

    pack = _export("job-sep")
    bare = {k: v for k, v in pack.items() if k != "ledger"}
    pems = "".join(ledger_keys.pem(ledger_keys.find_key(kid)[0])
                   for kid in pack["ledger"]["statement"]["signer_key_ids"] + pack["ledger"]["checkpoint"]["signer_key_ids"])
    done = _run(tmp_path, _write(tmp_path / "bare.json", {"pack": bare}),
                "--receipt", _write(tmp_path / "receipt.json", pack["ledger"]),
                "--keys", _write(tmp_path / "keys.pem", pems))
    assert [_lines(done.stdout)[c] for c in OFFLINE_CHECKS] == ["passed"] * 5, done.stdout
    assert done.returncode == 2
    # Without the receipt this pack fails: its own body says it was issued with signatures.
    alone = _run(tmp_path, str(tmp_path / "bare.json"), "--keys", str(tmp_path / "keys.pem"))
    assert _lines(alone.stdout)["signatures"] == "FAILED" and "its receipt was removed" in alone.stdout
    assert alone.returncode == 1 and "result: FAILED" in alone.stdout and "HMAC ONLY" not in alone.stdout
    # A pack from before the ledger is HMAC only: nothing is checked, nothing is claimed.
    from services.signed_proof_pack import sign_body

    old = sign_body({"version": 1, "job_id": "job-old"}, subject="job-old")
    before = _run(tmp_path, _write(tmp_path / "old.json", old), "--keys", str(tmp_path / "keys.pem"))
    assert set(_lines(before.stdout).values()) == {"not checked"} and "result: HMAC ONLY" in before.stdout
    assert before.returncode == 2 and "VERIFIED" not in before.stdout


def test_pinning_the_logs_key_id_is_enough_and_the_server_is_not_asked_for_keys(ledger, tmp_path):  # noqa: F811
    """The trust root: a key id obtained out of band; the keys themselves come with the receipt."""
    pack = _export("job-pin")
    pack_file = _write(tmp_path / "pack.json", pack)
    log_ids = pack["ledger"]["checkpoint"]["signer_key_ids"]
    pins = [arg for key_id in log_ids for arg in ("--pin", key_id)]
    done = _run(tmp_path, pack_file, *pins, "--json")
    result = json.loads(done.stdout)
    statuses = {c["id"]: c["status"] for c in result["checks"]}
    assert [statuses[c] for c in ("content_digest", "statement", "signatures", "inclusion",
                                  "checkpoint_signature")] == ["passed"] * 5, result["checks"]
    assert {s["trust"].split(" (")[0] for s in result["signatures"]} == {"the log published this key"}
    assert done.returncode == 2
    # A pin that is not the log's key leaves everything unchecked; it never passes.
    other = _run(tmp_path, pack_file, "--pin", "ab" * 32)
    rows = _lines(other.stdout)
    assert rows["signatures"] == rows["checkpoint signature"] == "not checked" and other.returncode == 2
    # The key event removed from the receipt: the workspace's signature cannot be checked from the pin.
    cut = copy.deepcopy(pack)
    cut["ledger"]["key_sets"] = []
    without = _run(tmp_path, _write(tmp_path / "cut.json", cut), *pins)
    assert _lines(without.stdout)["signatures"] == "not checked"
    assert _lines(without.stdout)["checkpoint signature"] == "passed"
    # A substituted key under the pinned id is not the pinned key.
    swapped = copy.deepcopy(pack)
    donor = swapped["ledger"]["key_sets"][0]["statement"]["key_set"]["keys"][0]
    swapped["ledger"]["checkpoint"]["keys"][0] = {**donor, "kid": swapped["ledger"]["checkpoint"]["keys"][0]["kid"]}
    forged = _run(tmp_path, _write(tmp_path / "swapped.json", swapped), *pins)
    assert _lines(forged.stdout)["checkpoint signature"] == "FAILED" and forged.returncode == 1
    bad = _run(tmp_path, pack_file, "--pin", "not-a-key-id")
    assert bad.returncode == 64 and "64 hex" in bad.stderr


def _assert_offline_sound(tmp_path: Path, name: str, pack: dict, workspace: str, kind: str) -> None:
    done = _run(tmp_path, _write(tmp_path / f"{name}.json", pack), "--keys",
                _write(tmp_path / f"{name}-keys.json", _jwks(workspace)), "--json")
    result = json.loads(done.stdout)
    statuses = {c["id"]: c["status"] for c in result["checks"]}
    assert [statuses[c] for c in ("content_digest", "statement", "signatures", "inclusion",
                                  "checkpoint_signature")] == ["passed"] * 5, (name, result["checks"])
    assert kind in next(c["detail"] for c in result["checks"] if c["id"] == "statement")
    assert done.returncode == 2 and result["verdict"] == "incomplete"


def test_a_parity_pack_from_the_real_sign_off_verifies_offline(ledger, store, tmp_path):  # noqa: F811
    from services.parity_signoff import exported_pack

    pack = exported_pack(store.get_job(_signed(store)), actor="viewer")
    _assert_offline_sound(tmp_path, "parity", pack, "w", "parity_report")


def test_an_exit_pack_from_the_real_sign_off_verifies_offline(ledger, proven, tmp_path):  # noqa: F811
    from services import exit_store
    from services.exit_evidence import exported_pack

    _sign_all(proven["id"])
    pack = exported_pack(exit_store.get(exit_store.REHEARSALS, proven["id"]), actor="viewer")
    _assert_offline_sound(tmp_path, "exit", pack, "ws1", "exit_evidence")


def test_tampered_inputs_fail_and_untrusted_keys_are_never_a_pass(ledger, tmp_path):  # noqa: F811
    pack = _export("job-tamper-cli")
    keys_file = _write(tmp_path / "keys.json", _jwks("w1"))

    def run_on(mutate, name: str) -> tuple[int, dict[str, str], str]:
        forged = copy.deepcopy(pack)
        mutate(forged)
        done = _run(tmp_path, _write(tmp_path / f"{name}.json", forged), "--keys", keys_file)
        return done.returncode, _lines(done.stdout), done.stdout

    code, rows, out = run_on(lambda p: p.update(actor="mallory"), "body")
    assert code == 1 and rows["content digest"] == "FAILED" and "result: FAILED" in out
    code, rows, out = run_on(lambda p: p.update(job_id="job-other"), "subject")
    assert code == 1 and rows["content digest"] == "FAILED" and rows["statement"] == "FAILED"
    code, rows, out = run_on(lambda p: p["ledger"]["statement"].update(issued_at="2001-01-01T00:00:00+00:00"), "stmt")
    assert code == 1 and rows["signatures"] == "FAILED" and rows["inclusion in the log"] == "FAILED"

    def flip_signature(p):
        sig = p["ledger"]["signatures"][0]
        sig["value"] = ("A" if sig["value"][0] != "A" else "B") + sig["value"][1:]

    code, rows, out = run_on(flip_signature, "sig")
    assert code == 1 and rows["signatures"] == "FAILED" and rows["inclusion in the log"] == "passed"
    code, rows, out = run_on(lambda p: p["ledger"]["signatures"].pop(), "dropped")
    assert code == 1 and rows["signatures"] == "FAILED"
    code, rows, out = run_on(lambda p: p["ledger"].update(leaf_index=p["ledger"]["leaf_index"] + 1), "index")
    assert code == 1 and rows["inclusion in the log"] == "FAILED"
    code, rows, out = run_on(lambda p: p["ledger"]["checkpoint"].update(tree_size=1000), "size")
    assert code == 1 and rows["checkpoint signature"] == "FAILED"
    code, rows, out = run_on(lambda p: p["ledger"].pop("inclusion_proof"), "noproof")
    assert code == 1 and rows["inclusion in the log"] == "FAILED"

    # No keys, or keys that are not the signers': "not checked", and never a pass.
    pack_file = _write(tmp_path / "pack.json", pack)
    nokeys = _run(tmp_path, pack_file)
    rows = _lines(nokeys.stdout)
    assert rows["signatures"] == rows["checkpoint signature"] == "not checked"
    assert rows["content digest"] == rows["inclusion in the log"] == "passed"
    assert nokeys.returncode == 2 and "VERIFIED" not in nokeys.stdout
    _export("job-elsewhere", "w9")
    wrong = _run(tmp_path, pack_file, "--keys", _write(tmp_path / "wrong.json",
                                                       {"keys": [k for k in _jwks("w9")["keys"]
                                                                 if k["datawrap_role"] == "statement"]}))
    assert _lines(wrong.stdout)["signatures"] == "not checked" and wrong.returncode == 2
    # A statement key relabelled as the checkpoint signer's does not make it one.
    lying = _jwks("w1")
    for key in lying["keys"]:
        key["datawrap_role"] = "statement"
    relabelled = _run(tmp_path, pack_file, "--keys", _write(tmp_path / "lying.json", lying))
    assert _lines(relabelled.stdout)["checkpoint signature"] == "FAILED" and relabelled.returncode == 1
    # A key file whose key id is not the hash of its key is refused outright.
    bad = _jwks("w1")
    bad["keys"][0]["kid"] = "0" * 64
    refused = _run(tmp_path, pack_file, "--keys", _write(tmp_path / "badkid.json", bad))
    assert refused.returncode == 64 and "could not be used" in refused.stderr


def test_a_saved_checkpoint_shows_the_log_only_grew_or_exposes_a_rewrite(ledger, tmp_path):  # noqa: F811
    first = _export("job-a")
    held = _write(tmp_path / "held.json", first["ledger"]["checkpoint"])  # the auditor keeps this
    keys_file = _write(tmp_path / "keys.json", _jwks("w1"))
    second = _export("job-b")
    sizes = first["ledger"]["checkpoint"]["tree_size"], second["ledger"]["checkpoint"]["tree_size"]
    proof = _write(tmp_path / "proof.json", ledger.consistency_proof(*sizes))
    pack_file = _write(tmp_path / "second.json", second)
    grown = _run(tmp_path, pack_file, "--keys", keys_file, "--previous-checkpoint", held,
                 "--consistency-proof", proof)
    assert _lines(grown.stdout)["consistency"] == "passed" and grown.returncode == 2, grown.stdout
    assert "(6 passed, 0 failed, 2 not checked)" in grown.stdout
    alone = _run(tmp_path, pack_file, "--keys", keys_file, "--previous-checkpoint", held)
    assert alone.returncode == 64 and "go together" in alone.stderr

    # The operator rewrites the first entry, drops the checkpoints and re-signs the history.
    path = tmp_path / "data" / "evidence_ledger.json"
    data = json.loads(path.read_text())
    target = data["evidence_ledger_entries"][str(first["ledger"]["leaf_index"])]
    target["statement"]["content_sha256"] = "e" * 64
    target["leaf_hash"] = verifier.leaf_hash(verifier.statement_bytes(target["statement"])).hex()
    data["evidence_ledger_checkpoints"] = {}
    path.write_text(json.dumps(data))
    ledger.reset_cache()
    third = _export("job-c")
    forged_proof = _write(tmp_path / "forged-proof.json",
                          ledger.consistency_proof(sizes[0], third["ledger"]["checkpoint"]["tree_size"]))
    third_file = _write(tmp_path / "third.json", third)
    unaware = _run(tmp_path, third_file, "--keys", keys_file)
    assert [_lines(unaware.stdout)[c] for c in OFFLINE_CHECKS] == ["passed"] * 5  # looks sound on its own
    caught = _run(tmp_path, third_file, "--keys", keys_file, "--previous-checkpoint", held,
                  "--consistency-proof", forged_proof)
    assert _lines(caught.stdout)["consistency"] == "FAILED" and caught.returncode == 1
    assert "rewritten" in caught.stdout and "result: FAILED" in caught.stdout


def test_a_withdrawal_receipt_turns_the_verdict_to_withdrawn(ledger, tmp_path):  # noqa: F811
    from services.ledger_packs import withdraw

    pack, other = _export("job-wd-cli"), _export("job-kept")
    out = withdraw(ledger.find_entry(pack["content_sha256"]), reason="Superseded by the re-run.",
                   superseded_by=None, actor="lead@example.com")
    keys_file = _write(tmp_path / "keys.json", _jwks("w1"))
    receipt_file = _write(tmp_path / "withdrawal.json", out["receipt"])
    done = _run(tmp_path, _write(tmp_path / "pack.json", pack), "--keys", keys_file, "--withdrawal", receipt_file)
    assert done.returncode == 3 and "result: WITHDRAWN" in done.stdout, done.stdout
    assert "Issued, later withdrawn at " in done.stdout and "Superseded by the re-run." in done.stdout
    assert _lines(done.stdout)["withdrawal"] == "passed"
    # Without it the tool cannot know, and says so rather than "issued".
    blind = json.loads(_run(tmp_path, str(tmp_path / "pack.json"), "--keys", keys_file, "--json").stdout)
    assert blind["issuance"] == "unknown" and blind["verdict"] == "incomplete"
    # A withdrawal of another pack, or one whose signature was altered, is not accepted.
    wrong = _run(tmp_path, _write(tmp_path / "other.json", other), "--keys", keys_file, "--withdrawal", receipt_file)
    assert wrong.returncode == 1 and _lines(wrong.stdout)["withdrawal"] == "FAILED"
    forged = copy.deepcopy(out["receipt"])
    forged["statement"]["withdrawal"]["reason"] = "never mind"
    altered = _run(tmp_path, str(tmp_path / "pack.json"), "--keys", keys_file,
                   "--withdrawal", _write(tmp_path / "forged-wd.json", forged))
    assert altered.returncode == 1 and _lines(altered.stdout)["withdrawal"] == "FAILED"


@pytest.mark.parametrize("args, message", [
    (["missing.json"], "could not be read as JSON"),
    (["notjson.txt"], "could not be read as JSON"),
    (["list.json"], "must be a JSON object"),
    (["pack.json", "--keys", "missing-keys.json"], "could not be used"),
    (["pack.json", "--keys", "notjson.txt"], "could not be used"),
])
def test_unusable_input_is_exit_64_with_a_reason(tmp_path, args, message):
    _write(tmp_path / "notjson.txt", "{not json")
    _write(tmp_path / "list.json", [1, 2])
    _write(tmp_path / "pack.json", {"content_sha256": "0" * 64, "signature": {"value": "x"}, "job_id": "j"})
    done = _run(tmp_path, *args)
    assert done.returncode == 64 and message in done.stderr and done.stdout == ""
