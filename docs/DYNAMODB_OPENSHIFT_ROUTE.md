# DynamoDB → OpenShift — client route (honest)

OpenShift is **not a database**. It is the hosting plane. The destination is
the store that runs *on* the cluster — almost always PostgreSQL via
[CloudNativePG](https://cloudnative-pg.io/) or
[Crunchy PGO](https://www.crunchydata.com/products/crunchy-postgresql-for-openshift).
MySQL and MongoDB operators are the same pattern: Service DNS, not the
OpenShift API.

DataFlow plants this as a **hosted alias of PostgreSQL**, not a second writer.

## Can we migrate?

**Yes — item snapshot.** DynamoDB `Scan` with `ConsistentRead=true` (AWS
backup/replication guidance) through the existing DynamoDB reader, then upsert
into PostgreSQL by HASH/RANGE. That is the same wire as a CNPG Service
(`orders-pg.payments.svc.cluster.local:5432`) or a laptop port-forward.

**No — “100% of DynamoDB the platform”.** That would include Streams CDC
exactly-once, GSI/LSI copies, TTL, global-table topology, and attributes the
application parked in S3 because of the 400 KB item cap. Those are not this
route. CDC default remains **at-least-once upsert**.

`100%` means every row on the named fixture
`apps/api/tests/test_dynamodb_openshift_fidelity_matrix.py` — HASH+RANGE, S/N/B/BOOL,
SS/NS/BS, nested M/L, explicit NULL vs missing, dest COUNT + Gate-8. Not AWS
production live (no credentials here). Not a Kubernetes API write.

## What competitors do

| Product | Algorithm | Honesty gap |
|---|---|---|
| Estuary | Scan backfill + DynamoDB Streams | Streams is continuous; S3 export is not relational |
| AWS DMS / Hevo | Scan / export + flatten | Nested M/L become JSON or explode; GSI not a table |
| Airbyte DynamoDB | Scan | Eventual consistent by default — can silently miss a write |
| Fivetran | Managed extract | Same document→relational tax |

DataFlow's wedge: **consistent Scan**, union of sparse keys, explicit NULL vs
missing, typed SS/NS/BS envelopes, dest-engine COUNT / Gate-8, OpenShift
resolved as PostgreSQL hosting — never invent create-new into etcd.

## Operator path

1. Source: DynamoDB table (AWS or DynamoDB Local).
2. Destination: catalog tile **OpenShift PostgreSQL (CNPG / Crunchy)** — driver
   is `postgresql`. Host = Service DNS, Route, or `127.0.0.1` port-forward.
   Optional extras: `openshift_service` + `openshift_namespace`.
3. Map: HASH (and RANGE) as primary key. Nested maps stay JSON unless flatten
   is approved.
4. Validate / Execute: fail-closed preflight, quarantine bad cells, checksum
   reconcile.
5. Cutover: application Query/GetItem rewrite is **out of band** — we move
   items, not DynamoDB access patterns.

Owner: `services.openshift_dest` (hosting) + `connectors.dynamodb_reader` (Scan).
`services.semantic_mapper.map_columns` remains Map SSOT.
