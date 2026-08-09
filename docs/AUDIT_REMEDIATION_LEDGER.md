# Audit Remediation Ledger

Branch: `fix/audit-p0-remediation` (from `devin/deep-audit-1784855991` @ `b1193a1`).

Status values: `NOT_STARTED` | `IN_PROGRESS` | `DONE_VERIFIED` | `BLOCKED` | `REGRESSED` | `DISPUTED`

| item | status | files changed | tests added | verify output | notes |
|------|--------|---------------|-------------|---------------|-------|
| 1 | IN_PROGRESS | — | — | — | Prior invent/ddl_type work existed; SA CREATE path still narrowed bare `integer`→INT32. Retracted any incomplete completion claim. |
| 2 | NOT_STARTED | | | | |
| 3 | NOT_STARTED | | | | |
| 4 | NOT_STARTED | | | | |
| 5 | NOT_STARTED | | | | |
| 6 | NOT_STARTED | | | | |
| 7 | NOT_STARTED | | | | |
| 8 | NOT_STARTED | | | | |
| 9 | NOT_STARTED | | | | |
| 10 | NOT_STARTED | | | | |
| 11 | NOT_STARTED | | | | |
| 12 | NOT_STARTED | | | | |
| 13 | NOT_STARTED | | | | |
| 14 | NOT_STARTED | | | | |
| 15 | NOT_STARTED | | | | |
| 16 | NOT_STARTED | | | | |
| 17 | NOT_STARTED | | | | |
| 18 | NOT_STARTED | | | | |
| 19 | NOT_STARTED | | | | |
| 20 | NOT_STARTED | | | | |
| 21 | NOT_STARTED | | | | |
| 22 | NOT_STARTED | | | | |
| 23 | NOT_STARTED | | | | |
| 24 | NOT_STARTED | | | | |
| 25 | NOT_STARTED | | | | **NEW (not in original audit):** Excel→PG empty cells fail with `Empty value cannot coerce to decimal/datetime` + FAIL_JOB; 1921 quarantined; minio ImportError suppressed. |

## Flow track

```
devin/deep-audit-1784855991 (b1193a1)
        └── fix/audit-p0-remediation  ← ITEM work lands here
```
