# Third-party licences

This page covers the licences of the software Datawrap depends on: what was
scanned, what needs attention, and how CI keeps it current. The attributions
themselves are in [`NOTICE`](../../NOTICE) at the repository root.

## Scope and method

- **Python.** Every distribution reachable from `apps/api/requirements.txt`
  (shipped in the API image) and `apps/api/requirements-dev.txt` (test tools,
  not shipped). Licences come from the installed package metadata
  (`License-Expression`, licence classifiers, the `License` field). The
  optional extras `requirements-rag.txt` and `requirements-databricks.txt`
  are not scanned, because they are not part of the default install.
- **npm.** Every package in `package-lock.json`. The lockfile records each
  package's licence and whether it is a dev dependency. When the lockfile has
  no licence, the scanner reads the package's own `package.json`.
- **Tool.** `scripts/licence_scan.py`. The policy is in `licence-policy.json`.

Scan of 2026-11-13: 493 packages.

| Ecosystem / scope | Allowed | Copyleft | Unknown or proprietary |
|---|---|---|---|
| Python, shipped | 180 | 5 | 3 |
| Python, test only | 39 | 1 | 0 |
| npm, shipped in the web bundle | 62 | 0 | 0 |
| npm, build and test only | 203 | 0 | 0 |

## Copyleft, proprietary and unknown licences

| Package | Shipped | Licence | Assessment |
|---|---|---|---|
| `paramiko` | yes | LGPL-2.1 | SFTP connector. Used unmodified, as a separately replaceable library. **Owner decision.** |
| `psycopg2-binary` | yes | LGPL-3.0 with exceptions | PostgreSQL driver. The wheel bundles libpq and OpenSSL. Used unmodified. **Owner decision.** |
| `pymssql` | yes | LGPL-2.1 | SQL Server fallback driver. The wheel bundles FreeTDS (LGPL). Used unmodified. **Owner decision.** |
| `certifi` | yes | MPL-2.0 | CA bundle. File-level copyleft, used unmodified. |
| `orjson` | yes | MPL-2.0 AND (Apache-2.0 OR MIT) | Pulled in by `trino`. Used unmodified. |
| `hdbcli` | yes | Proprietary (SAP) | SAP HANA client. Redistribution in the image is **not cleared**. **Owner decision.** |
| `teradatasql` | yes | Proprietary (Teradata) | Teradata driver. Redistribution in the image is **not cleared**. **Owner decision.** |
| `pypdfium2` | yes | BSD-3-Clause / Apache-2.0 plus bundled-library licences | The PDFium binary's dependencies are permissive; their texts ship in the wheel. |
| `hypothesis` | no (test only) | MPL-2.0 | Not distributed. |

No GPL, AGPL or SSPL dependency was found. The shipped web bundle has no
copyleft package. Its fonts (`@fontsource/*`) are under OFL-1.1, which
allows bundling as long as the copyright and licence travel with the fonts;
`NOTICE` carries both.

## Decisions for the owner

1. **The project's own licence.** The repository has no `LICENSE` file, and
   the root `package.json` declares none. `NOTICE` therefore says nothing
   about the project's own terms. Choosing them is the owner's call.
2. **LGPL drivers in the image** (`paramiko`, `psycopg2-binary`, `pymssql`).
   Using an unmodified LGPL library is normally compatible with proprietary
   distribution, as long as the licence text, a source pointer, and the
   user's ability to replace the library are preserved. Python packages in a
   container meet the last condition. Legal sign-off is still needed.
3. **Proprietary drivers in the image** (`hdbcli`, `teradatasql`). Shipping
   them inside a container may not be allowed by their vendor terms. The
   options are to keep them with a confirmed right to redistribute, to make
   them drivers the operator installs, or to remove them.

Each of these has `owner_decision: true` in `licence-policy.json`. The flag
is there so CI stays green. It is not an approval.

## The CI gate

The `security` job in `.github/workflows/ci.yml` runs:

```
python scripts/licence_scan.py --check --check-notice NOTICE
```

The job fails when:

- a dependency has a copyleft (GPL, AGPL, LGPL, SSPL, MPL, EPL, ...) or
  unknown licence and no reviewed exception in `licence-policy.json`;
- a reviewed package's licence changes from the one it was reviewed under;
- a direct production dependency is missing from `NOTICE`.

The full report is uploaded as the `licence-report` artifact. After adding a
dependency, regenerate the attributions with:

```
python scripts/licence_scan.py --notice NOTICE
```

Run it with the API's Python environment, from the repository root.
