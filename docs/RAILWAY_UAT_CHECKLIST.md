# Railway pre-production UAT checklist

Use this after each deploy to Railway. Hard-refresh the browser first so the new web bundle loads.

## 0. Deploy sanity

- [ ] API health: `GET /health` returns operational
- [ ] Web loads Transfer Studio (no stale cached bundle — hard refresh)
- [ ] Sign in works; saved connectors still list

## 1. Source preview must show **rows**, not only columns

For each source below: select connector → enter table/collection → wait for right-side preview.

| Source | Object | Expect |
|--------|--------|--------|
| Snowflake | known non-empty table | Field chips **and** sample table with values |
| PostgreSQL / MySQL | known table | Same |
| MongoDB | known collection | Same (table or JSON toggle) |
| DynamoDB | known table | Same |

**Fail if:** right side shows only column chips and “no sample rows” / empty grid.  
**Action:** click **Reload sample preview**, check Railway API logs for `sample read failed` / warehouse / schema.

## 2. Validate dry-run (the prior Snowflake→Mongo block)

- [ ] Source: Snowflake table with data  
- [ ] Dest: MongoDB collection  
- [ ] Map → Validate  
- [ ] Dry-run / integrity is **Passed** (not “No sample rows available”)  
- [ ] Execute completes; docs appear in Mongo

## 3. Bidirectional smoke (pick one row per pair)

| # | Source → Destination | Validate | Execute |
|---|----------------------|----------|---------|
| A | Snowflake → MongoDB | ☐ | ☐ |
| B | MongoDB → Snowflake | ☐ | ☐ |
| C | PostgreSQL (or MySQL) → Snowflake | ☐ | ☐ |
| D | Snowflake → PostgreSQL (or MySQL) | ☐ | ☐ |
| E | MongoDB → DynamoDB | ☐ | ☐ |
| F | DynamoDB → Snowflake (or Mongo) | ☐ | ☐ |

## 4. Destination preview

- [ ] Mongo / SQL dest introspect shows existing schema or clear “will create” message  
- [ ] No fake “650+ live” claims in marketing if browsing public pages

## 5. Ops / integrity (quick)

- [ ] Overview shows jobs; CDC lag / DLQ only if relevant  
- [ ] Failed transfer surfaces quarantine, not silent drop  
- [ ] Re-run Validate after fixing mapping still works

## Pass criteria for this UAT cycle

1. No connector shows **columns-only** preview when the table has data.  
2. Snowflake → Mongo Validate is unblocked with sample rows.  
3. At least pairs **A + B + C** execute successfully on Railway.

## If something fails

1. Note connector name, table, screenshot of Source preview + Validate gate.  
2. Railway API log lines around `POST /api/v1/transfer/introspect` and `preflight`.  
3. Confirm warehouse/schema/role for Snowflake; collection name for Mongo.
