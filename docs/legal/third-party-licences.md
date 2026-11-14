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
     - `freetds-<version>.tar.bz2`. pymssql's wheel links FreeTDS (LGPL)
       statically; the version is read from the linked binary (1.5.19 in
       pymssql 2.4.6).

     The PyPI sources are verified against PyPI's SHA-256 digests. FreeTDS
     is verified against a SHA-256 pinned per version in `UPSTREAM`
     (`scripts/bundled_libraries.py`), taken from a source independent of
     freetds.org, so the download host cannot also vouch for itself: for
     1.5.19, Homebrew's formula at commit 7cac0676. A linked version without a pin fails the
     build. Downloads are HTTPS only, and a redirect to a non-HTTPS URL is
     refused. Any failure fails the image build.
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
listing every bundled library. The exception is the GCC source, which is in
the companion source bundle:

| Source | Covers | Verified against |
|---|---|---|
| keyutils 1.5.10 | the pymssql copy | CentOS Stream dist-git SHA-512 (lookaside cache), pinned |
| keyutils 1.5.8 | the psycopg2-binary copy | pinned MD5 published in Arch Linux's 1.5.8-1 PKGBUILD (2013), the only checksum ever published for 1.5.8 (Fedora / CentOS 7 recorded MD5 / SHA-1; Debian never packaged it). The CI `api-image` job records its SHA-256 after the MD5 check and fails if it changes |
| libxcrypt 4.5.2 | psycopg2-binary's libcrypt | pinned SHA-256, the same value manylinux pins |
| GCC 8.5.0 (el8 tarball, about 65 MB; **companion bundle**) | numpy's libquadmath and libgfortran | pinned SHA-512 from CentOS Stream c8s |

How the libxcrypt version was identified: the library carries no version
string. It is libxcrypt, not glibc, because it exports the `crypt_gensalt`
family under glibc-compatible symbol versions, which is how the manylinux2014
image builds it. It was compiled by clang 22.1.8, which points to a 2026
manylinux image, and those pin libxcrypt 4.5.2. That mapping is recorded per
bundled file, keyed by its auditwheel name `libcrypt-bc2db45e.so.1`
(`WHEEL_LIB_VERSIONS`). The hash suffix follows the library's content, so a
rebuilt libcrypt fails the build with "review bundled libraries" until it is
reviewed, whichever wheel version carries it.

The review is hermetic. `apps/api/constraints-image.txt` locks every package
the image installs, and `REVIEWED_WHEELS` in `scripts/bundled_libraries.py`
records the version of each bundling wheel that was reviewed. A wheel that
bundles libraries at any other version fails both the image build and the CI
licence gate with "review bundled libraries". A test also requires the lock
and `REVIEWED_WHEELS` to agree, so a lock bump cannot skip the review.

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

**Where the GCC source is.** The GCC 8.5.0 source for numpy's
libquadmath and libgfortran is about 65 MB. Under the owner's
companion-bundle decision (2026-11-13), it is not in the runtime image. It
is in the companion source bundle (see "Companion source bundle" below). Its
`UPSTREAM` entry says `bundle="companion"`. The runtime's
`BUNDLED_LIBRARIES.json` and NOTICE name "companion bundle <tag>-sources" as
its location.

`ibm_db` is no longer shipped. Its wheel bundled IBM's proprietary
`clidriver` (about 120 MB, outside `.libs/`) and keyutils 1.6.3. Under
owner decision 3 it is operator-installed, so it is excluded from this scan
and from NOTICE (`OPERATOR_INSTALLED` in `scripts/bundled_libraries.py`).

## Companion source bundle (owner decision, 2026-11-13)

Large copyleft source is not in the runtime image. It goes in a companion
source bundle, published with every release.

**What stays in the runtime image.** The image keeps the small Python-wheel
LGPL sources, so it stays self-describing:
- paramiko, psycopg2 and pymssql;
- FreeTDS, keyutils and libxcrypt;
- the bundled-library inventory.

They are in `/usr/share/doc/datawrap/third-party-source/`, with
`SHA256SUMS`, `MANIFEST.json` and `BUNDLED_LIBRARIES.json`.

**What moves to the companion.** The companion is an OCI image:
- built `FROM scratch` (the `sources` stage of `Dockerfile.api`);
- named `<registry>/<image>:<tag>-sources`, for example
  `ghcr.io/<owner>/<repo>/api:v1.2.3-sources`.

It holds only:
- the Debian source of every copyleft system package in the runtime image;
- the GCC 8.5.0 source;
- `SHA256SUMS`, `DEBIAN_SOURCES.json` and `MANIFEST.json`.

`release.yml` publishes it in the same job, the same registry and with the
same tag plus `-sources`. It is pushed before the runtime image. Before the
release is created, the job checks that the companion is published, and
that the runtime image's label and `SOURCES.md` name it. If the companion is
not published, the release fails. This is the GPL model of "equivalent
access to copy the source from the same place". The GitHub release notes
name the companion too.

**The pointer.** The runtime image says exactly where its companion is:
- `/usr/share/doc/datawrap/SOURCES.md` gives the companion reference for
  this tag and how to copy it (`docker create` + `docker export`,
  `sha256sum -c SHA256SUMS`). It lists every Debian source package, with
  the binary packages it covers, and every file, with its SHA-256;
- the label `io.datawrap.sources` holds the companion
  reference;
- `/usr/share/doc/datawrap/companion/` holds a copy of the companion's
  `SHA256SUMS` and manifests.

The runtime build runs `scripts/source_bundle.py --check`. It fails unless
the companion holds exactly two things:
- the runtime's copyleft Debian source packages, recomputed from the
  runtime's own `dpkg-query` and copyright files;
- the inventory's companion-located wheel-library sources.

Builds outside a release (local, CI) keep the default `SOURCES_REF`, which
says the build is unpublished.

## Debian packages in the API image

The API image is Debian bookworm (`python:3.12-slim-bookworm`). It ships
the base image's packages plus the runtime stage's `unixodbc`, `libodbc2`
(LGPL-2.1, for pyodbc) and `curl` (health check), with their dependencies.

**Multi-stage build.** `Dockerfile.api` builds the Python environment
(`/opt/venv`) in a `build` stage that has `build-essential` and `libpq-dev`.
The runtime stages (`runtime-base`, `runtime`) copy `/opt/venv` and install
only the runtime packages above, so no compiler or header package ships.
The runtime requirements need no compiler (four sdists are pure Python:
clickhouse-sqlalchemy, pure-sasl, pydruid, pyhive), but they are built in
the build stage all the same. The runtime pip installs are wheels only:
- the vendor drivers, with `--only-binary :all:`;
- preflight, which is pure Python.

**Source collection.** The `sources-collect` stage is built FROM
`runtime-base`, so it has exactly the runtime's packages. It runs
`scripts/fetch_debian_sources.py --dest /sources` in five steps:
1. List every installed package with `dpkg-query`, with its exact source
   version.
2. Read each package's licence from `/usr/share/doc/<pkg>/copyright`. A
   package whose copyright file is missing, or names no licence the script
   recognises, fails the build.
3. For every package whose copyright file names a copyleft licence (GPL,
   LGPL, MPL, ...), collect the `.dsc`, orig and debian tarballs of the
   exact source version. The files come from deb.debian.org and are checked
   against apt's signed Sources index: the step adds `deb-src` and runs
   `apt-get update`, which verifies the archive signature. When a version
   has left the archive, they come from snapshot.debian.org, checked against
   its content address. Every file is also checked against the `.dsc`
   checksums.
4. Merge the files' sums into `SHA256SUMS`.
5. List package, source, files and how each was verified in
   `DEBIAN_SOURCES.json`.

Any failure fails the build. On the 2026-11-13 base image, 96 of its 105
packages (67 source packages) have a copyleft copyright file. The 9
permissive ones are debconf, libmd0, the ncurses packages, libpcre2-8-0,
tzdata and zlib1g. Their sources (glibc, GCC 12's runtime libraries, perl,
systemd, util-linux, ...) are a few hundred MB, now in the companion and not
the runtime image.

**Drift checks** (CI security job, offline):
- `fetch_debian_sources.py --verify-dockerfile` fails in three cases. The
  runtime stages install an apt package that `licence-policy.json`
  `apt_packages` has not reviewed, or no longer install a listed one, or
  install a compiler or a `-dev` package. Copyleft entries must say
  "companion bundle <tag>-sources"; the licence gate rejects any other text.
- `source_bundle.py --verify-dockerfile` checks the companion layout:
  - the companion step lists exactly the companion-located families in
    `UPSTREAM` (today `gcc`);
  - the companion stage runs the Debian step, which covers every copyleft
    apt package (exactness is checked at build time, see above);
  - `sources` is `FROM scratch` and holds only `/sources`;
  - the runtime stages run neither step, copy only the manifests, set the
    label and write the pointer.

**CI image job.** The `api-image` job builds both images without a layer
cache.

The runtime image must have:
- no `build-essential` or `gcc`;
- the small wheel sources, matching `SHA256SUMS`;
- no Debian `.dsc`, no GCC source and no `DEBIAN_SOURCES.json`;
- the `SOURCES.md` pointer, naming the companion, and the label;
- a passing `pip check` and working core imports.

The companion is exported, and every file must match its `SHA256SUMS`. That
file must equal the runtime's copy.

**keyutils 1.5.8.** keyutils 1.5.8 has only a published MD5, and no mirror
of it is reachable from the development sandbox. So the same job re-checks
the MD5 of the tarball in the runtime image. It records the tarball's
SHA-256 in the Actions cache on the first run and fails if a later build's
differs. Committing that value as `UPSTREAM` `sha256` makes the build
itself enforce it.

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
