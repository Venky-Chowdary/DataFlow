# QA batch 1: backlog IDs reused for different defects

Several fixes from QA batch 1 (fix/qa-batch1 and PR #435, 21 Nov 2026) name
backlog story IDs in commit messages, test names and docstrings, but fix a
different defect from the backlog story with that ID. Those backlog stories
are still open. Do not close one on this evidence.

- **D396 / D377** in QA batch 1: servers that force a PRIMARY KEY (MySQL
  `sql_require_primary_key`, MariaDB `innodb_force_primary_key`): carried
  source key, approved `_dw_row_id` surrogate, keyed side tables. Not the
  backlog D396 or D377 stories.
- **D407** in QA batch 1: a retry from start refused on the destination table
  the same job had just created ("dest schema drift"). Not the backlog D407
  story.
- **D344** (part) in QA batch 1: the schedule form's disabled Save button now
  says why and marks the field. Not the backlog D344 story.
- **D47** in QA batch 1: Snowflake SCD2 and mirror predicates emitted
  `IS [NOT] TRUE`, which Snowflake rejects (001003). Not the backlog D47 story.

New tests, docstrings and commit messages call these "QA batch 1 item N".

## Verified against

- `apps/api/tests/test_d396_require_primary_key_surrogate_e2e.py` (QA batch 1: servers that force a primary key)
- `apps/api/tests/test_train1_r2_forced_pk_probe_unreadable.py`
- `apps/api/tests/test_qa_nov21_defects.py` (QA batch 1 retry, schedule Save and Snowflake predicate items)
