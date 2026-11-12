# Datawrap Helm chart: operator notes

The chart's settings are documented next to each value in `values.yaml`. This
file covers what an upgrade or a rollback does to the control-plane stores.

## Control-plane stores

Tenant settings (custom domain, IP allowlist, require-SSO, session policy),
BYOK key records, resource ACL grants, notification channels, shape drafts,
preflight runs, CDC mapping reviews, MCP invocations and audit-tip anchors are
control-plane stores. Every replica must see the same values, so they live in
the metadata database (MongoDB or DocumentDB) rather than in one file per pod:

| Collection              | Holds                                                                 |
|-------------------------|-----------------------------------------------------------------------|
| `control_plane_items`   | One document per record of the record stores (tenants, BYOK key records, ACL grants, notification channels, shape drafts), `_id` `<store>:<record id>` |
| `control_plane_records` | Log stores (preflight runs, CDC reviews, MCP log, audit-tip anchors) |
| `control_plane_docs`    | One status document per store, import claims, conflict notes        |

Records are written one at a time by compare-and-swap, so tenants never
contend with each other and no store is bound by MongoDB's 16 MB document
limit. A record is at most 1 MB; a shape draft's canvas layout at most 256 KB
(the API answers 413 above that), with at most 50 drafts per workspace.

## Upgrading from a build that kept these stores in files

On the first start of this build, the first process to reach an empty shared
store imports its node's file for that store (for example `tenants.json` or
`resource_acls.jsonl` under the data directory). The file is never deleted or
rewritten. While that import runs, other processes refuse to read the store
(503) rather than serve a half-imported ACL or tenant list; a process that
dies mid-import finishes it when it starts again with the same file.

Only one file per store is imported. Once the shared store holds a store's
records, a node's file is never imported or merged again. A file that differs
(another replica's own copy from before the upgrade, or a file an old build
wrote during a rollback) is **conflicting local data**:

- it is logged once, as a warning that names the store, the file and its
  SHA-256, and as the `control_store.local_file_conflict` security event;
- it is recorded in the store's status: one document per file content in
  `control_plane_docs` with `kind: "local_file_conflict"`, the store name, the
  path, the hash, the pod name and the time;
- the shared store stays authoritative. Nothing in the file is applied.

If several replicas kept different files before the upgrade, check the
conflict records after the first start and re-create any tenant, grant, key or
channel that only existed on another replica through the UI or API.

## Rolling back to a build that keeps these stores in files

A rollback does not carry the shared state back into the files:

- The old build serves each pod's files as they were at the upgrade, plus its
  own later writes. Everything changed on this build since the upgrade is
  missing on the old one: a revoked ACL grant is granted again, a deleted
  tenant, key or channel is back, a new one is absent.
- A BYOK key created or rotated after the upgrade is unknown to the old build,
  so secrets wrapped with it cannot be decrypted there. Do not rotate BYOK keys
  until the upgrade is final.

Changes made on the old build during a rollback are **not carried forward**
when you upgrade again: the changed files are recorded as conflicting local
data and are not imported (see above). Re-apply such changes after the
upgrade.

### Export before a rollback

Keep a copy of the shared state before rolling back, so nothing changed since
the upgrade is lost and it can be re-applied:

```sh
for c in control_plane_items control_plane_records control_plane_docs; do
  mongoexport --uri "$MONGODB_URI" --collection "$c" --out "$c.json"
done
```

Store the export like any other backup: BYOK key records and notification
channel settings are encrypted at rest, but the export still holds tenant
security settings.
