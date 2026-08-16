# Test / Local Service Credentials (audit sandbox)

These are brought up natively (no Docker) for live transfer/CDC proof.

## PostgreSQL (logical replication enabled)
- host: localhost  port: 5432
- db: dataflow  user: dataflow  password: dataflow  (SUPERUSER)
- wal_level=logical, max_replication_slots=10, max_wal_senders=10
- start: `service postgresql start`

## MongoDB (single-node replica set rs0 — change streams work)
- uri: mongodb://localhost:27017
- start: `mongod --dbpath /data/db --replSet rs0 --bind_ip_all --fork --logpath /var/log/mongo/mongod.log`
- init once: `mongosh --eval 'rs.initiate({_id:"rs0",members:[{_id:0,host:"localhost:27017"}]})'`

## Redis
- localhost:6379 (no auth) — CDC leases / worker fleet
- start: `service redis-server start`

## Notes
- Test harness auto-skips live PG tests unless dataflow/dataflow on :5432 authenticates.
- SQL Server / Oracle / Snowflake / cloud object stores are NOT available on this
  ARM sandbox; related tests skip or fail on driver/service absence (see audit).
