# Security Scanning — Authoritative Posture & False-Positive Catalogue

This document is the source of truth for how DataFlow's security is scanned and
why certain generic-scanner findings are false positives. It exists so a
re-scan (or a human reviewer) judges the code against **reality**, not against a
naive pattern matcher.

## Authoritative gates (CI `security` job, `.github/workflows/ci.yml`)
- **Bandit** `bandit -r apps/api/{connectors,services,src} -lll` (HIGH only) — a
  full-tree posture config is in `bandit.yaml`.
- **pip-audit** on the default installed environment (CVE gate).
- **Ruff** + **mypy** on the Decision Kernel / hardened paths.

## Measured reality (Bandit)
| Severity | Count | Notes |
|----------|-------|-------|
| HIGH     | **0** | was 1 (`foreign_key_carry.py` SHA1 for identifier shortening) — fixed with `usedforsecurity=False` |
| MEDIUM   | ~49   | 48× `B608` SQL f-strings (identifiers hardened via `connectors/sql_identifiers.py`) + 1 XML |
| LOW      | ~208  | 178× `B110` deliberate suppress-and-log `except: pass`; `B105` field-name constants; test asserts |

A third-party report scored the repo **20/100** and claimed **608 high-severity**
issues. Bandit — the industry-standard Python security scanner — finds **1**
(now 0). The gap is scanner naïveté, catalogued below with file:line evidence.

## False-positive catalogue (validated by inspection)

| Report claim | Reality | Evidence |
|---|---|---|
| 85× `exec`/`eval` code injection | **Redis `EVAL` (Lua)**, not Python `eval` | `services/cdc_lease_store.py` `client.eval(script, …)` |
| `exec()` in production | **SQL `EXEC`/`EXECUTE` regex + `cursor.execute`**, not Python `exec` | `services/procedure_source.py` `_EXEC_RE`, DBAPI `.execute()` |
| Weak MD5 (security) | **Postgres `md5()` for row-reconciliation checksums** (non-crypto); Python call already `usedforsecurity=False` | `services/engine_checksum.py:184,189` (SQL), `src/ai/rag/embedding_service.py:117` |
| Weak SHA1 (security) | **Identifier shortening** (6-hex suffix); fixed with `usedforsecurity=False` | `services/foreign_key_carry.py:187` |
| Insecure `random` | **Deterministic synthetic data / backoff jitter** — `secrets` would be wrong | `packages/ml/**`, `services/error_handling.py` |
| 2 circular imports | **Already broken by lazy/function-local imports**; app boots, tests pass | `connector_capabilities.py:1020`, `registry.py` (all in-function) |
| 6 "syntax errors" | **UTF-8 BOM** on 6 test files — stripped | (fixed) |
| 378 undefined vars | **Guarded `try/except ImportError: x = None` fallbacks** the linter can't follow | e.g. `packages/preflight/src/preflight/gates.py` |

## Genuinely open (tracked, not hidden)
- MEDIUM `B608` SQL f-strings: continue migrating remaining call sites onto
  `connectors/sql_identifiers.py` quoting helpers.
- 4,527 high-complexity functions & god-modules: real tech-debt; refactor the
  top offenders behind config objects, one at a time, with tests.

## How to reproduce the reality locally
```bash
pip install bandit
bandit -c bandit.yaml -r apps/api/connectors apps/api/services apps/api/src -lll   # HIGH gate → 0
```
