# Migration rules

These are the rules every Datawrap transfer follows. They are the vendor and
standards behaviour a principal data architect holds a pipeline to, and they
are stricter than the defaults Fivetran, Airbyte and Informatica ship where
those products widen silently, round decimals through float, or treat their
own previous write as foreign drift.

Each production fix names the rule it applies.

## 1. Type mapping keeps precision, scale, length and fractional seconds

A type moves to a carrier that can store every source value and every value
already in the destination. Precision, scale, length and fractional seconds
are part of the type, not a display hint.

| Engine | Exact numeric | Character | Timestamp fractional seconds |
| --- | --- | --- | --- |
| PostgreSQL | `numeric(p,s)` exact, up to 131072 digits before the point. `real` / `double precision` are IEEE-754 and inexact. `timestamp(p)` allows `p` 0..6 (microseconds). | `varchar(n)` / `text` are Unicode. `character varying` with no length is unlimited. | [PostgreSQL numeric types](https://www.postgresql.org/docs/current/datatype-numeric.html), [date/time types](https://www.postgresql.org/docs/current/datatype-datetime.html) |
| SQL Server | `decimal`/`numeric` are exact. `float` is approximate. | `varchar`/`char` without `n` in a data definition or variable is length **1**. `nvarchar` is UTF-16. Changing `nvarchar` to `varchar` can drop characters outside the code page. `varchar(max)` / `nvarchar(max)` are the LOBs. | `datetime2(p)` and `datetimeoffset(p)` allow `p` 0..7 (100 ns). The default is 7. [char and varchar](https://learn.microsoft.com/en-us/sql/t-sql/data-types/char-and-varchar-transact-sql), [datetime2](https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql), [ALTER TABLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql) |
| Oracle | `NUMBER(p,s)` is exact decimal. `BINARY_DOUBLE` is IEEE-754. | A zero-length string is NULL (rule 3). `NVARCHAR2` / `NCLOB` are the national types. | `TIMESTAMP` fractional seconds default to 6 and max at 9. [Nulls](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Nulls.html) |
| MySQL / MariaDB | `DECIMAL(p,s)` is exact fixed-point. `DOUBLE` is approximate. `fsp` on `DATETIME`/`TIMESTAMP` is 0..6; omitted `fsp` means 0, not the SQL default of 6. | `utf8mb4` is the Unicode character set. National `NVARCHAR` is an alias of `utf8mb3` (BMP only). | [Date and time type syntax](https://dev.mysql.com/doc/refman/8.4/en/date-and-time-type-syntax.html), [fixed-point types](https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html) |
| Snowflake | `NUMBER(p,s)` precision up to 38. `FLOAT` is approximate. | `VARCHAR` is Unicode. | `TIMESTAMP_NTZ` / `TIMESTAMP_TZ` fractional seconds are 0..9, default 9. [Numeric types](https://docs.snowflake.com/en/sql-reference/data-types-numeric), [date and time types](https://docs.snowflake.com/en/sql-reference/data-types-datetime) |
| MongoDB | `Decimal128` is the exact decimal. Do not put a decimal through IEEE double. | Strings are UTF-8. | BSON Date is milliseconds since the epoch. A microsecond source loses digits unless the operator accepts that carrier or the value is stored as a string. [BSON types](https://www.mongodb.com/docs/manual/reference/bson-types/) |
| Kafka | The wire type is the schema (JSON, Avro, Protobuf). A decimal is an exact logical type or a string, never a JSON float. | | |
| CSV / TSV / SFTP | No native types. Exact numerics stay decimal strings. Dates stay the source text unless a typed carrier was declared. | Rule 9. | |

`datetime2(7)` into MySQL `DATETIME(6)` or PostgreSQL `timestamp(6)` drops the 100-nanosecond digit. That is a fidelity risk at plan time and at execute time, for a new table and for an existing one. A value whose 7th digit is 0 is not lossy at the value level; `.1234567` is. The operator accepts the truncation explicitly or picks a carrier that holds 100 ns (a string, or a separate digit column). Snowflake `TIMESTAMP_NTZ(7)` holds the SQL Server value.

Fivetran promotes a column to the most specific type that losslessly holds both the old and the new data, and does not apply a narrowing. Airbyte's declared schema is the contract. Informatica's high-precision mode keeps Decimal as Decimal up to 28 (or 38) digits; with high precision off, decimals become doubles and round at 15 digits. Datawrap never applies a narrowing, and never takes the Informatica low-precision path.

Sources: [Fivetran schema management](https://fivetran.com/docs/connector-sdk/building-connectors/connector-sdk-concepts/schema-management), [How Fivetran works](https://fivetran.com/docs/core-concepts), [Informatica Decimal high precision](https://docs.informatica.com/data-integration/powercenter/10-5-10/advanced-workflow-guide/high-precision-data/decimal.html).

## 2. No narrowing or destructive DDL without approval and a backup

An `ALTER` runs only when the new type can hold every value the column already stores and every value this run will write. Same family, or a strictly wider family. Unicode stays Unicode: `NVARCHAR(n)` becomes `NVARCHAR(m)` with `m >= n`, or `NVARCHAR(MAX)`. It never becomes `VARCHAR`.

An unsized character type is never emitted. On SQL Server that spelling is length 1.

SQL Server `NVARCHAR` and `NCHAR` without `MAX` stop at 4000. `VARCHAR` and `CHAR` stop at 8000. A source `VARCHAR(n)` with `n > 4000`, and a source `VARCHAR(MAX)`, map to `NVARCHAR(MAX)` on SQL Server. They never map to `NVARCHAR(5000)`, which is not a legal non-MAX length ([nchar and nvarchar](https://learn.microsoft.com/en-us/sql/t-sql/data-types/nchar-and-nvarchar-transact-sql)).

`ALTER COLUMN` on SQL Server restates `NULL` or `NOT NULL` and `COLLATE`. Omitting them makes the column nullable and replaces the collation with the database default ([ALTER TABLE](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql)). The restore statement is the original type plus those two clauses, captured before the widen. The widen statement restates them too, so the widen itself does not drop them.

Oracle `VARCHAR2(n)` is `BYTE` unless `CHAR` is written. Under AL32UTF8 a character can take 4 bytes, so `VARCHAR2(800 BYTE)` holds 200 characters and is not a widen of `VARCHAR2(400 CHAR)` ([Oracle data types](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Data-Types.html)). The comparison uses character capacity: `CHAR` counts as characters, `BYTE` counts as `n / 4`.

MySQL `VARCHAR(n)` counts characters. The row-size limit and the InnoDB index limit count bytes: 65,535 bytes per row and 3,072 bytes per index, and utf8mb4 uses up to 4 bytes per character ([CHAR and VARCHAR](https://dev.mysql.com/doc/refman/8.4/en/char.html), [InnoDB limits](https://dev.mysql.com/doc/refman/8.4/en/innodb-limits.html)). A character length that fits the type can still overflow the row.

A widen of a column Datawrap did not create, where the source type already fits the live column, does not run. SQL Server's create-new spelling of integer is `BIGINT`; that is not a reason to alter an `INT` column whose source is `INTEGER` / `INT4`.

The widen is committed before rows are written, including on PostgreSQL. A transaction rollback does not undo it. A failed run that committed no rows issues a compensating `ALTER` back to the captured definition on every engine. MySQL commits DDL implicitly, so that compensating statement is the only restore ([MySQL implicit commit](https://dev.mysql.com/doc/refman/8.4/en/implicit-commit.html)). A restore that fails fails the job and is written to the audit log. A table the run created, with nothing committed, is dropped on cancel. A table the customer already had is never dropped.

Fivetran does not apply narrowing type changes. Datawrap matches that and also refuses the unsized-character footgun Microsoft documents.

## 3. NULL and empty string

SQL uses three-valued logic. `NULL <op> x` is UNKNOWN, so the row does not match. `NULL = NULL` is not true. `NULL <> x` is not true. Only `IS NULL` / `IS NOT NULL` test for null. PostgreSQL states this directly: ordinary comparison operators yield null when either input is null ([comparison functions](https://www.postgresql.org/docs/current/functions-comparison.html), [logical operators](https://www.postgresql.org/docs/current/functions-logical.html)). ISO/IEC 9075 is the same rule.

`''` is a value on PostgreSQL, SQL Server, MySQL, MariaDB, Snowflake and in CSV. It is not NULL. Whitespace is not NULL. The text `'NULL'` and `'none'` are not NULL.

Oracle is the exception: a character value of length zero is NULL ([Oracle Nulls](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/Nulls.html)). Plans that touch Oracle say so. An empty string written to Oracle becomes NULL, and a read of that column comes back as NULL. The pipeline must not pretend it round-tripped `''`.

A blank-as-missing probe used for type inference is the wrong predicate for a SQL filter. Float NaN is not NULL.

MongoDB `$ne` and `$nin` match documents where the field is null or missing ([$ne](https://www.mongodb.com/docs/manual/reference/operator/query/ne/)). A not-equal or not-in filter therefore adds an existence and non-null guard, and the plan warns that NULL rows are dropped. That matches SQL, where `NULL <> x` is unknown.

## 4. Primary keys and MongoDB `_id`

The destination key is the declared key, an explicit operator mapping, or a source column that is already that key (`id` mapped to `id`, `_id` mapped to `_id`). A fuzzy name match or a type match must not land an unrelated column on `_id`, on an identity column, or on a primary key.

MongoDB `_id` is immutable and unique ([documents](https://www.mongodb.com/docs/manual/core/document/)). The MongoDB Kafka sink notes that an ObjectId or UUID strategy can only promise at-least-once, because a retry mints a new id; a strategy that reuses a unique business key can be exactly-once ([sink id strategy](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/configuration-properties/id-strategy/)). Datawrap upserts on the declared key and does not invent `_id` from another column.

An omitted column is not part of the write and is not part of read-back verification.

## 5. Idempotency, delivery and watermarks

Kafka's own exactly-once applies to Kafka-to-Kafka transactions. A consumer that writes to another system and then commits offsets gets at-least-once if it crashes in between; committing offsets first gets at-most-once ([Kafka design](https://kafka.apache.org/39/design/design/)). Datawrap commits source offsets only after the destination commit. The default statement to operators is **at-least-once upsert** until an exactly-once proof exists for that route.

Airbyte documents the same at-least-once incremental guarantee and tells sources to resend a row when the cursor is ambiguous ([incremental append](https://docs.airbyte.com/platform/using-airbyte/core-concepts/sync-modes/incremental-append)).

A modification watermark is exclusive on the high side (`cursor > last`), with the primary key as a tie-breaker so two rows that share the boundary timestamp are not skipped and the boundary row is not read again. A schema change does not rewind the cursor. A refused run does not reset the watermark. A re-snapshot says so on the job.

Fivetran invalidates cursors only when the operator asks for a re-sync, and then says the load is a full reload ([How Fivetran works](https://fivetran.com/docs/core-concepts)).

## 6. Upsert and dedupe

Upsert is an idempotent merge on the declared key: one row per key, last writer wins, disclosed.

- PostgreSQL: `INSERT ... ON CONFLICT`
- SQL Server: `MERGE` ([MERGE](https://learn.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql))
- MySQL / MariaDB: `INSERT ... ON DUPLICATE KEY UPDATE`
- Snowflake: `MERGE` ([MERGE](https://docs.snowflake.com/en/sql-reference/sql/merge))

`incremental_append` appends one new version per change, including a new version of a key that is already present. That is Airbyte's incremental append, not deduped history. It behaves the same way on every run. A duplicate-key guard is a different, declared mode (`incremental_deduped` or upsert) and is applied on run 1 if it is applied on run 3.

A file has a defined row order. Overwrite plus an explicit dedupe key collapses to the last row and says so. A table with no `ORDER BY` does not have an order, so it may still refuse rather than pick an arbitrary copy.

`skip_duplicates` keeps the stored row and counts `rows_skipped`.

## 7. CDC ordering

Debezium's PostgreSQL connector takes a consistent snapshot, records the WAL LSN, then streams commits from that LSN. Events for a table stay in LSN order ([Debezium PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)). Airbyte CDC does the same shape: a snapshot, then the log from the stored position ([Airbyte CDC](https://docs.airbyte.com/platform/understanding-airbyte/cdc)).

Datawrap applies the snapshot first, then change events in commit order. A column added on the source is added on the destination before events that carry it are applied, or the schedule parks on a schema-drift finding. Events are not applied while a new column is dropped on the floor. A missing nested field is NULL, in CDC and in overwrite. A mapping review blocks the schedule and table it belongs to, not every other schedule on the connector.

## 8. Reversibility and rollback

A transfer that requires reversibility is refused at plan and at schedule create when a step cannot be undone (`omit`, `mask_pii`, a destructive type change). Execute checks the same predicate. An unattended run must not be the first place the operator learns it will fail.

Cancel stops the writer before the next row. The per-row fallback checks the flag too. Source transactions roll back and return the connection; no `idle in transaction` session is left holding locks. A table this job created, with zero committed rows, is dropped. Offsets and watermarks move only with a committed destination batch (rule 5).

## 9. CSV and TSV quoting

CSV follows [RFC 4180](https://www.rfc-editor.org/rfc/rfc4180): records are separated by CRLF; a field that contains comma, CR, LF or double-quote is enclosed in double-quotes; a quote inside a field is doubled. Spaces are data. Datawrap uses that dialect for CSV, including CRLF inside a quoted field, which is data and is preserved.

NULL is an empty unquoted field. `''` is `""`. The literal text `NULL` is quoted when it would otherwise be read as the null marker.

TSV quoting is a Datawrap product choice, not RFC 4180. The RFC specifies CSV. The product TSV uses the same quote rule with tab as the delimiter and CRLF as the record terminator: a field that contains a tab, quote, CR or LF is quoted, and `''` is `""`, not the same bytes as NULL. Appending to a TSV written before that dialect (LF records, or a raw mid-field quote) is refused. The existing object is left as it is; the operator overwrites it. On append to a CSV, the existing file's line ending is matched when it is uniform, and a mix of LF and CRLF is refused. Fast exports to S3 use CRLF, including PostgreSQL `COPY` output, which the server writes with LF and the product rewrites before the upload.

PostgreSQL `COPY` CSV uses the same distinction: NULL is the null string and is not quoted; a non-NULL value that matches the null string is quoted ([COPY](https://www.postgresql.org/docs/current/sql-copy.html)).

JSON and JSONL write an exact decimal as a string or as a decimal-aware JSON number, never via `float()`. A null column is the JSON `null`, including JSON/JSONB columns. The verifier fingerprints the source value, so a rounded float cannot agree with itself.

## 10. Decimals never pass through float

`99999999999.999999` is `NUMERIC(18,6)`. IEEE-754 double cannot hold it; `float()` yields `100000000000.0`. PostgreSQL says `real` and `double precision` are inexact. Informatica documents the same loss when high precision is off. MongoDB metadata and JSONL use `Decimal128` or a decimal string. A float or a decimal measure is not a payment-card candidate: a PAN is a 13 to 19 digit integer with no decimal point.

## 11. Honest trust and audit reporting

A check reports the rows this run wrote, after the same filter and the same transforms the writer applied, at the carrier precision the operator accepted (BSON milliseconds, `Decimal128`, `datetime2` to `timestamp(6)` once accepted). It does not compare a two-row delta to the whole table, a raw source value to a transformed destination, or a keyless append table as if it had one row per key.

A correct run is not marked failed. A wrong run still fails, and the message names the key. Rows this pipeline wrote are not "changed outside this pipeline". A column this pipeline added is part of the next baseline. `rebaseline_transfer` moves the baseline the next run uses.

The plan and execute run the same gates on the same inputs. A green plan does not fail at execute for a reason that was knowable. Error text names the cause and a remedy that exists in the product UI and in the MCP tools. No raw driver exception, no internal hostname, no invented job from an empty request.

Catalog count is not transfer-live. Only a route with `TRANSFER_READY` / `PRODUCTION_SKU` evidence is described as live. CDC is at-least-once upsert until an exactly-once proof is attached.
