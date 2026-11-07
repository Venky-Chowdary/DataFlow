# Audit chain: read-only production check (D221)

QA saw 1,371 audit-chain verification failures on production. This check shows
how many of them were *written* that way, not changed afterwards:

* **Forks**: two records claiming the same predecessor (`prev_hash`). The API,
  worker and scheduler all appended to one chain, and before D221 the only lock
  was per process, so two processes could read the same tip and both link to it.
* **Records signed under a different key**: before D221 every record was
  HMAC-ed with the login secret (`DATAFLOW_AUTH_SECRET`), so rotating that
  secret made every older record fail verification.

Neither of those means a record was altered. The check **never writes**: it
runs only `find`, `aggregate` (with no `$out`/`$merge`) and `count`, and it
creates no index, collection or file. It changes no audit record.

## Option A: the script

Run it from `apps/api` with the production Mongo URI in the environment (the
same variable the API uses):

```sh
cd apps/api
MONGODB_URI='<production mongo uri>' python scripts/audit_chain_check.py --db datatransfer
```

Add `--json` for machine-readable output. To also find out which key
reproduces each record's HMAC, export the keys the API uses and add `--hmac`:

```sh
MONGODB_URI='<production mongo uri>' \
DATAFLOW_AUTH_SECRET='<current login secret>' \
DATAFLOW_AUDIT_HMAC_KEY='<audit chain key, if set>' \
DATAFLOW_AUDIT_HMAC_RETIRED_KEYS='<previous login secret(s), comma-separated>' \
python scripts/audit_chain_check.py --db datatransfer --hmac
```

Key material is never printed; keys appear only as their public id (`ak_…`).

How to read it:

| Output | Means |
| --- | --- |
| `predecessors claimed more than once` | Number of forks: predecessors that two or more records link to. `extra record(s)` is how many records sit on a second branch. |
| `duplicate chain positions` | Positions (`chain_seq`) held by more than one record. The same race leaves these behind. |
| `records per signing key` | `legacy (no key_id)` = written before D221, signed with the login secret. `ak_…` = signed with that chain key. |
| `records not under the current key` | Records whose `key_id` is not the key in `DATAFLOW_AUDIT_HMAC_KEY`. |
| `hmac verified under legacy login secret` / `retired key` | Untouched records signed with an older secret. |
| `hmac verified under no configured key` | The only count that can mean an altered record. It also includes records signed with a secret you did not export (for example, a login secret rotated before D221). |

If the fork count plus the records that only verify under an older secret
roughly add up to the failures QA saw, the failures came from how records were
written, not from tampering.

## Option B: mongosh (equivalent queries)

Use a read-only user if you have one. Every query below only reads.

```js
const c = db.getSiblingDB("datatransfer").audit_events;

// Total records, and records per chain format version (null = before R7).
c.countDocuments({});
c.aggregate([{ $group: { _id: "$chain_v", n: { $sum: 1 } } }]);

// Forks: predecessors claimed by more than one record.
c.aggregate([
  { $match: { prev_hash: { $type: "string", $ne: "" } } },
  { $group: { _id: "$prev_hash", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
  { $group: { _id: null, predecessors_claimed_more_than_once: { $sum: 1 },
              records_beyond_first_claim: { $sum: { $subtract: ["$n", 1] } } } }
], { allowDiskUse: true });

// Duplicate chain positions.
c.aggregate([
  { $match: { chain_seq: { $gt: 0 } } },
  { $group: { _id: "$chain_seq", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
  { $count: "duplicate_positions" }
], { allowDiskUse: true });

// Forks on per-workspace lines.
c.aggregate([
  { $match: { ws_prev_hash: { $type: "string", $ne: "" } } },
  { $group: { _id: "$ws_prev_hash", n: { $sum: 1 } } },
  { $match: { n: { $gt: 1 } } },
  { $count: "workspace_line_forks" }
], { allowDiskUse: true });

// Records per signing key (null = legacy, signed with the login secret).
c.aggregate([
  { $group: { _id: { $ifNull: ["$key_id", "legacy (no key_id)"] }, n: { $sum: 1 } } },
  { $sort: { n: -1 } }
]);

// Records not signed with the current chain key. Get CURRENT_KEY_ID from
// GET /api/v1/audit/verify -> signing.current_key_id, or from the script.
c.countDocuments({ key_id: { $ne: "CURRENT_KEY_ID" } });
```

mongosh cannot recompute HMACs without the secret, so the `--hmac` breakdown is
script-only. The structure counts (forks, duplicate positions, records per key)
are identical between the two options.

## After the check

* Set `DATAFLOW_AUDIT_HMAC_KEY` to a strong random value, the same on the API,
  worker, scheduler and CDC worker services. If it is unset, each process
  generates a key in its data directory instead (never the login secret), and
  records from processes on other hosts then show as `key_mismatch`.
* If the login secret was rotated before D221, put the previous value(s) in
  `DATAFLOW_AUDIT_HMAC_RETIRED_KEYS` so those records verify again.
* `GET /api/v1/audit/verify` now reports `categories` (`tampering`, `fork`,
  `key_mismatch`, `legacy_hash_input`) and `tampering_detected`.
