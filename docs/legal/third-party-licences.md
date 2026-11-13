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
| `certifi` | yes | MPL-2.0 | CA bundle. File-level copyleft, used unmodified. MPL-2.0 then requires only notice of where the source is available, which `NOTICE` gives; no source is shipped. |
| `orjson` | yes | MPL-2.0 AND (Apache-2.0 OR MIT) | Pulled in by `trino`. Used unmodified; as for `certifi`, the source-availability notice in `NOTICE` is what MPL-2.0 requires. |
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
   - the corresponding source ships **in the image**, so no ongoing
     obligation (such as a written offer) is needed. `Dockerfile.api` runs
     `scripts/fetch_lgpl_sources.py` after installing the runtime
     requirements. It places, under `/usr/share/doc/datawrap/third-party-source/`,
     the source of exactly the installed versions, with `SHA256SUMS` and
     `MANIFEST.json`:
     - `paramiko-<version>.tar.gz`;
     - `psycopg2-<version>.tar.gz` (the source of `psycopg2-binary`);
     - `pymssql-<version>.tar.gz`;
     - `freetds-<version>.tar.gz`. pymssql's wheel links FreeTDS (LGPL)
       statically; the version is read from the linked binary.

     The PyPI sources are verified against PyPI's SHA-256 digests. FreeTDS
     is verified against freetds.org's published SHA-256 when there is one,
     and is otherwise labelled in `MANIFEST.json` as recorded at download.
     Any failure fails the image build.
   - `NOTICE` carries, for each, the LGPL licence-text URL, where in the image
     the source is, and that the library is separately replaceable. The gate
     refuses an approved GPL-family exception whose notice lacks the
     licence-text URL or "source shipped in image at …", or that still
     mentions a written offer.
   - CI (`security` job, `fetch_lgpl_sources.py --verify-dockerfile`) fails
     in three cases:
     - the Dockerfile's package list differs from the approved LGPL entries;
     - one of those packages is not a runtime requirement;
     - an installed version has no source distribution on PyPI.

     The versions cannot drift: the step reads them from the installed
     packages.
   - **Libraries bundled inside the wheels.** With `--bundled`, the same
     step also ships the source of the LGPL/GPL native libraries that the
     installed manylinux wheels carry in `<package>.libs/`. See the next
     section.
3. **Proprietary drivers** (`hdbcli`, `teradatasql`; and `ibm-db`, whose
   wheel bundles IBM's proprietary Db2 `clidriver`). Decided 2026-11-13:
   operator-installed (the same rule was applied to `ibm-db` once its
   `clidriver` was found). They are no longer in `apps/api/requirements.txt` or
   the production image, and no longer in `licence-policy.json` (Datawrap
   does not distribute them). See the next section.

No owner decision is pending (`owner_decision: true` appears nowhere in
`licence-policy.json`).

## Native libraries bundled inside the wheels

Every manylinux wheel in the runtime set was inventoried on 2026-11-13. The
list was read from the cp312 wheels on PyPI, and it matches the wheels
installed here. `scripts/bundled_libraries.py` maps each bundled library to
its licence. An unreviewed library fails the image build, the CI drift check
and the licence gate, and so does a copyleft library whose source cannot be
named.

| Wheel | Bundled libraries | Licences |
|---|---|---|
| psycopg2-binary 2.9.13 | libpq, OpenSSL 3 (libssl, libcrypto), MIT Kerberos (krb5, k5crypto, gssapi, krb5support, com_err), OpenLDAP (ldap, lber), Cyrus SASL, PCRE, libselinux, **keyutils 1.5.8**, **libcrypt (libxcrypt 4.5.2)** | PostgreSQL, Apache-2.0, MIT, OLDAP-2.8, CMU, BSD-3-Clause, public domain, **LGPL-2.1+** |
| pymssql 2.4.6 | OpenSSL 1.1.1k, MIT Kerberos, PCRE2, libselinux, **keyutils 1.5.10**; FreeTDS 1.5.19 linked statically | OpenSSL/SSLeay, MIT, BSD-3-Clause, public domain, **LGPL-2.1+**, **LGPL-2.0+** |
| numpy 2.5.4 | OpenBLAS, **GCC 8.5.0 libquadmath**, **libgfortran** | BSD-3-Clause, **LGPL-2.1+**, **GPL-3.0+ with the GCC Runtime Library Exception** |
| pillow 13.0.0 | FreeType, HarfBuzz, libjpeg-turbo, Little CMS, OpenJPEG, libpng, libtiff, libwebp/sharpyuv, libavif, Brotli, Zstandard, liblzma, libXau, libxcb | FTL, MIT, IJG/BSD/Zlib, BSD-2/3-Clause, libpng-2.0, libtiff, 0BSD |
| ijson 3.6.0 | YAJL | ISC |
| google-crc32c 1.9.0 | crc32c | BSD-3-Clause |

**Copyleft (bold).** Their corresponding source ships in
`/usr/share/doc/datawrap/third-party-source/`, with `BUNDLED_LIBRARIES.json`
listing every bundled library:

| Source | Covers | Verified against |
|---|---|---|
| keyutils 1.5.10 | the pymssql copy | CentOS Stream dist-git SHA-512 (lookaside cache), pinned |
| keyutils 1.5.8 | the psycopg2-binary copy | no published checksum found for 1.5.8, so the SHA-256 is recorded at download and labelled as such |
| libxcrypt 4.5.2 | psycopg2-binary's libcrypt | pinned SHA-256, the same value manylinux pins |
| GCC 8.5.0 (el8 tarball, about 65 MB) | numpy's libquadmath and libgfortran | pinned SHA-512 from CentOS Stream c8s |

How the libxcrypt version was identified: the library carries no version
string. It is libxcrypt, not glibc, because it exports the `crypt_gensalt`
family under glibc-compatible symbol versions, which is how the manylinux2014
image builds it. It was compiled by clang 22.1.8, which points to a 2026
manylinux image, and those pin libxcrypt 4.5.2. That mapping is recorded per
wheel version (`WHEEL_LIB_VERSIONS`), so a new psycopg2-binary release fails
the build until it is reviewed.

libgfortran's GCC Runtime Library Exception does not itself require source,
but it is covered by the same GCC tarball as libquadmath.

The shipped tarballs are upstream / RPM sources. The Red Hat build patches
live in the CentOS Stream dist-git repositories.

**Permissive.** Attribution, including the OpenSSL, CMU (Cyrus SASL),
FreeType and IJG notices, is in `NOTICE` under "Libraries bundled inside the
shipped Python wheels".

**Statically linked libraries (best effort).** `.libs/` only shows
libraries that are dynamically linked. The same step also scans every
extension module outside `.libs/` for version strings that well-known
libraries embed. The signatures are FreeTDS, keyutils, GNU Readline,
Libgcrypt, GnuTLS, GNU libiconv, OpenSSL, BoringSSL, zlib, SQLite, libpq
and curl (`STATIC_SIGNATURES`).

A copyleft hit fails the image build, CI and the licence gate unless its
source is shipped (`STATIC_MAPPED`; today only FreeTDS in pymssql). The
permissive hits are attributed in NOTICE:

| Wheel | Statically linked |
|---|---|
| cryptography | OpenSSL 4.0.3 |
| pyarrow | OpenSSL 3.6.2, curl 8.20.0, zlib 1.3.2 |
| grpcio | BoringSSL, zlib 1.3.1.1 |
| lxml, xmlsec | zlib |
| pymssql | FreeTDS 1.5.19 (LGPL; source shipped) |

A library that embeds no recognisable version string is not detected, so
this scan is a safety net, not a proof of absence.

**Image size.** Shipping the GCC 8.5.0 source for numpy's libquadmath and
libgfortran adds about 65 MB to the API image. The owner accepted this
(2026-11-13) as the cost of meeting the LGPL by shipping source instead of a
written offer.

`ibm_db` is no longer shipped. Its wheel bundled IBM's proprietary
`clidriver` (about 120 MB, outside `.libs/`) and keyutils 1.6.3. Under
owner decision 3 it is operator-installed, so it is excluded from this scan
and from NOTICE (`OPERATOR_INSTALLED` in `scripts/bundled_libraries.py`).

## Operator-installed vendor drivers (SAP HANA, Teradata, IBM Db2)

The SAP HANA, Teradata and IBM Db2 connectors need proprietary vendor drivers, plus
the SQLAlchemy dialect the generic SQL path dials through. Datawrap does not
ship any of them. The connectors show **"Requires operator-installed
driver"** in the connector catalog (`operator_installed_driver` on each
catalog entry) until an operator installs them.

| Connector | Packages | Vendor licence |
|---|---|---|
| SAP HANA (`sap_hana`, `hana`, `sap_bw_4hana`) | `hdbcli`, `sqlalchemy-hana` | SAP's licence for the HANA client; the dialect is Apache-2.0 |
| Teradata (`teradata`, `teradata_vantage`) | `teradatasql`, `teradatasqlalchemy` | Teradata's licence terms |
| IBM Db2 (`db2`, `ibm_db2`, `ibm_db2_warehouse`) | `ibm-db` (bundles IBM's `clidriver`), `ibm-db-sa` | IBM's licence and redistribution terms for the Db2 client; the dialect is Apache-2.0 |

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

CI does not install them: no test exercises a real SAP HANA, Teradata or Db2
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
