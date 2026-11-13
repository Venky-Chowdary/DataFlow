# Third-party licences

This page covers the licences of the software Datawrap depends on: what was
scanned, what needs attention, and how CI keeps it current. The attributions
themselves are in [`NOTICE`](../../NOTICE) at the repository root.

## Datawrap's own licence

Datawrap is proprietary software: Copyright (c) 2026 Venky Chowdary. All
rights reserved. The terms are in [`LICENSE`](../../LICENSE) at the repository
root: no licence is granted without the copyright holder's written
permission, and the software is provided as is. The project's own packages
declare it too: `"license": "UNLICENSED"` with `"private": true` in every
`package.json`, and `Proprietary` in `packages/preflight/pyproject.toml`.
The licence gate lists them as first-party (`first_party` in
`licence-policy.json`), so they are never scanned as dependencies, and fails
if one of them declares any other licence.

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

Scan of 2026-11-13 (after owner decision 3): 494 packages, of which 4 are
Datawrap's own (first-party).

| Ecosystem / scope | Allowed | Copyleft | Unknown or proprietary |
|---|---|---|---|
| Python, shipped | 179 | 5 | 1 |
| Python, test only | 39 | 1 | 0 |
| npm, shipped in the web bundle | 62 | 0 | 0 |
| npm, build and test only | 203 | 0 | 0 |

## Copyleft, proprietary and unknown licences

| Package | Shipped | Licence | Assessment |
|---|---|---|---|
| `paramiko` | yes | LGPL-2.1 | SFTP connector. Used unmodified, as a separately replaceable library. **Approved by the owner, 2026-11-13.** |
| `psycopg2-binary` | yes | LGPL-3.0 with exceptions | PostgreSQL driver. The wheel bundles libpq and OpenSSL. Used unmodified. **Approved by the owner, 2026-11-13.** |
| `pymssql` | yes | LGPL-2.1 | SQL Server fallback driver. The wheel bundles FreeTDS (LGPL). Used unmodified. **Approved by the owner, 2026-11-13.** |
| `certifi` | yes | MPL-2.0 | CA bundle. File-level copyleft, used unmodified. |
| `orjson` | yes | MPL-2.0 AND (Apache-2.0 OR MIT) | Pulled in by `trino`. Used unmodified. |
| `hdbcli` | **no**, operator-installed | Proprietary (SAP) | SAP HANA client. Removed from the image (owner decision, 2026-11-13); see below. |
| `teradatasql` | **no**, operator-installed | Proprietary (Teradata) | Teradata driver. Removed from the image (owner decision, 2026-11-13); see below. |
| `pypdfium2` | yes | BSD-3-Clause / Apache-2.0 plus bundled-library licences | The PDFium binary's dependencies are permissive; their texts ship in the wheel. |
| `hypothesis` | no (test only) | MPL-2.0 | Not distributed. |

No GPL, AGPL or SSPL dependency was found. The shipped web bundle has no
copyleft package. Its fonts (`@fontsource/*`) are under OFL-1.1, which
allows bundling as long as the copyright and licence travel with the fonts;
`NOTICE` carries both.

## Decisions for the owner

1. **The project's own licence.** Decided 2026-11-13: proprietary, all
   rights reserved (see above).
2. **LGPL drivers in the image** (`paramiko`, `psycopg2-binary`, `pymssql`).
   Decided 2026-11-13: accepted with attribution. Conditions, which the
   licence gate enforces through `licence-policy.json` (`decision`):
   - they stay unmodified and are loaded as separate, user-replaceable
     Python packages;
   - `NOTICE` carries, for each, the LGPL licence-text URL, the source
     location and a written offer of the corresponding source for the
     shipped version (binary distribution in the image). The gate refuses an
     approved GPL-family exception without those three.
3. **Proprietary drivers** (`hdbcli`, `teradatasql`). Decided 2026-11-13:
   operator-installed. They are no longer in `apps/api/requirements.txt` or
   the production image, and no longer in `licence-policy.json` (Datawrap
   does not distribute them). See the next section.

No owner decision is pending (`owner_decision: true` appears nowhere in
`licence-policy.json`).

## Operator-installed vendor drivers (SAP HANA, Teradata)

The SAP HANA and Teradata connectors need proprietary vendor drivers, plus
the SQLAlchemy dialect the generic SQL path dials through. Datawrap does not
ship any of them. The connectors show **"Requires operator-installed
driver"** in the connector catalog (`operator_installed_driver` on each
catalog entry) until an operator installs them.

| Connector | Packages | Vendor licence |
|---|---|---|
| SAP HANA (`sap_hana`, `hana`, `sap_bw_4hana`) | `hdbcli`, `sqlalchemy-hana` | SAP's licence for the HANA client; the dialect is Apache-2.0 |
| Teradata (`teradata`, `teradata_vantage`) | `teradatasql`, `teradatasqlalchemy` | Teradata's licence terms |

An operator who has accepted the vendor's licence installs them on the API
host, then restarts the API, in one of two ways:

```
pip install -r apps/api/requirements-vendor-drivers.txt
docker build -f Dockerfile.api --build-arg INSTALL_VENDOR_DRIVERS=1 .
```

The build-arg defaults to `0`, so the published image never contains them.
Without the drivers, connector Test, save and run refuse with one message,
naming the packages to install and the licence to accept. No connection is
attempted (`connectors/vendor_drivers.py`). With the drivers installed,
behaviour is unchanged.

CI does not install them: no test exercises a real SAP HANA or Teradata
server. The connector tests use stand-in modules.

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
