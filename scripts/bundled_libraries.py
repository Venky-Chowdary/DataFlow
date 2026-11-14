"""What the shipped Python wheels bundle in their ``<pkg>.libs/`` directories.

auditwheel copies the native libraries a manylinux wheel links into
``<pkg>.libs/``, renamed ``<lib>-<hash>.so...``. They ship in the API image
with the wheel, so each needs its licence honoured:

* permissive ones (OpenSSL, MIT Kerberos, libpq, ...) need attribution, which
  NOTICE carries (``scripts/licence_scan.py``);
* LGPL / GPL ones (keyutils, libxcrypt, GCC's libquadmath / libgfortran) need
  their corresponding source, which ``scripts/fetch_lgpl_sources.py --bundled``
  ships in the image.

Every library a wheel bundles must be in :data:`BUNDLED`, and every copyleft one
must resolve to an :data:`UPSTREAM` source; anything else fails the image
build and the CI check, so a new bundled library cannot slip in unreviewed.

Inventory of 2026-11-13 (runtime requirements, cp312 manylinux wheels, read
from the wheels on PyPI): google-crc32c, ijson, numpy, pillow, psycopg2-binary,
pymssql. ibm_db is operator-installed since owner decision 3 and not shipped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class BundledLibraryError(RuntimeError):
    """A bundled library is not reviewed, or its source cannot be named."""


@dataclass(frozen=True)
class Bundled:
    component: str
    licence: str
    copyleft: bool = False
    attribution: str = ""
    family: str = ""  # source family for copyleft libraries


_MIT_KRB = Bundled("MIT Kerberos (krb5)", "MIT")
_OPENSSL = Bundled(
    "OpenSSL",
    "Apache-2.0 (3.x) / OpenSSL and SSLeay licences (1.1.x)",
    attribution="This product includes software developed by the OpenSSL Project for use in the "
    "OpenSSL Toolkit (http://www.openssl.org/), and cryptographic software written by Eric Young.",
)
_WEBP = Bundled("libwebp", "BSD-3-Clause")
_BROTLI = Bundled("Brotli", "MIT")

BUNDLED: dict[str, Bundled] = {
    # copyleft: corresponding source ships in the image
    "libkeyutils": Bundled("keyutils", "LGPL-2.1-or-later", copyleft=True, family="keyutils"),
    "libcrypt": Bundled("libxcrypt", "LGPL-2.1-or-later", copyleft=True, family="libxcrypt"),
    "libquadmath": Bundled("GCC libquadmath", "LGPL-2.1-or-later", copyleft=True, family="gcc"),
    "libgfortran": Bundled(
        "GCC libgfortran", "GPL-3.0-or-later WITH GCC-exception-3.1", copyleft=True, family="gcc"
    ),
    # permissive: attribution in NOTICE
    "libcom_err": Bundled("com_err (MIT Kerberos / e2fsprogs)", "MIT"),
    "libcrypto": _OPENSSL,
    "libssl": _OPENSSL,
    "libgssapi_krb5": _MIT_KRB,
    "libk5crypto": _MIT_KRB,
    "libkrb5": _MIT_KRB,
    "libkrb5support": _MIT_KRB,
    "liblber": Bundled("OpenLDAP", "OLDAP-2.8"),
    "libldap": Bundled("OpenLDAP", "OLDAP-2.8"),
    "libpcre": Bundled("PCRE", "BSD-3-Clause"),
    "libpcre2-8": Bundled("PCRE2", "BSD-3-Clause WITH PCRE2-exception"),
    "libpq": Bundled("PostgreSQL libpq", "PostgreSQL"),
    "libsasl2": Bundled(
        "Cyrus SASL",
        "BSD-Attribution-HPND-disclaimer (CMU)",
        attribution="This product includes software developed by Computing Services at "
        "Carnegie Mellon University (http://www.cmu.edu/computing/).",
    ),
    "libselinux": Bundled("libselinux", "public domain"),
    "liblzma": Bundled("XZ Utils liblzma", "0BSD / public domain"),
    "libxml2": Bundled("libxml2", "MIT"),
    "libscipy_openblas64_": Bundled("OpenBLAS (scipy-openblas)", "BSD-3-Clause"),
    "libXau": Bundled("libXau", "MIT"),
    "libxcb": Bundled("libxcb", "MIT"),
    "libavif": Bundled("libavif", "BSD-2-Clause"),
    "libbrotlicommon": _BROTLI,
    "libbrotlidec": _BROTLI,
    "libbrotlienc": _BROTLI,
    "libfreetype": Bundled(
        "FreeType",
        "FTL",
        attribution="Portions of this software are copyright (c) The FreeType Project "
        "(www.freetype.org). All rights reserved.",
    ),
    "libharfbuzz": Bundled("HarfBuzz", "MIT-Modern-Variant"),
    "libjpeg": Bundled(
        "libjpeg-turbo",
        "IJG AND BSD-3-Clause AND Zlib",
        attribution="This software is based in part on the work of the Independent JPEG Group.",
    ),
    "liblcms2": Bundled("Little CMS", "MIT"),
    "libopenjp2": Bundled("OpenJPEG", "BSD-2-Clause"),
    "libpng16": Bundled("libpng", "libpng-2.0"),
    "libsharpyuv": _WEBP,
    "libwebp": _WEBP,
    "libwebpdemux": _WEBP,
    "libwebpmux": _WEBP,
    "libtiff": Bundled("libtiff", "libtiff"),
    "libzstd": Bundled("Zstandard", "BSD-3-Clause (dual-licensed; BSD terms used)"),
    "libyajl": Bundled("YAJL", "ISC"),
    "libcrc32c": Bundled("google/crc32c", "BSD-3-Clause"),
}


#: Wheels the operator installs (requirements-vendor-drivers.txt, owner
#: decision 3): never shipped in the image, so neither scanned nor sourced.
OPERATOR_INSTALLED: frozenset[str] = frozenset(
    {"hdbcli", "sqlalchemy-hana", "teradatasql", "teradatasqlalchemy", "ibm-db", "ibm-db-sa"}
)


@dataclass(frozen=True)
class Upstream:
    filename: str
    urls: tuple[str, ...]
    sha256: str = ""
    sha512: str = ""
    md5: str = ""  # only where no SHA-2 checksum was ever published (keyutils 1.5.8)
    note: str = ""
    #: "image": shipped in the runtime image; "companion": in the companion
    #: source bundle <image>:<tag>-sources (owner decision: large sources).
    bundle: str = "image"


#: Where a companion-located source is, as NOTICE, the policy and the
#: inventory say it. release.yml publishes <image>:<tag>-sources with each image.
COMPANION_LOCATION = "companion bundle <tag>-sources"
IMAGE_SOURCE_DIR = "/usr/share/doc/datawrap/third-party-source"


def source_location(key: tuple[str, str] | None) -> str:
    """Where the corresponding source of a bundled library is ("" when none is needed)."""
    if key is None:
        return ""
    if UPSTREAM[key].bundle == "companion":
        return COMPANION_LOCATION
    return f"image: {IMAGE_SOURCE_DIR}"


_LOOKASIDE = "https://sources.stream.centos.org/sources/rpms"
_KEYUTILS_1510 = (
    "7f6f956c7e76cdc2aeb52e74fe670b20a5f9a5d9b543fd2ce971d80c48745f37"
    "d05235a42f0a8f152b1128a109c7d8bf07e751282a20d2d3f433a99a5308ae8d"
)
_GCC_85_EL8 = (
    "4811dbcbdce8fe0fa8bc1fdca024988a1a75ac89e3efc675fe991b81032c220a"
    "b4bd1bc530bb99f6209ffeca67581a198de82f7253530de8c24a2a81770eb4ea"
)

#: (family, version) -> where its source comes from, with a pinned checksum
#: from a source independent of the download host. A family / version without
#: a pin fails the build (fetch_lgpl_sources.py). Checksums: CentOS Stream
#: dist-git ``sources`` files (keyutils c8s, gcc c8s), the manylinux libxcrypt
#: pin, Homebrew's FreeTDS formula and Arch Linux's keyutils 1.5.8 PKGBUILD,
#: all read on 2026-11-13.
UPSTREAM: dict[tuple[str, str], Upstream] = {
    ("keyutils", "1.5.8"): Upstream(
        "keyutils-1.5.8.tar.bz2",
        (
            "https://people.redhat.com/~dhowells/keyutils/keyutils-1.5.8.tar.bz2",
            # Fedora's lookaside cache, addressed by the same MD5.
            (
                "https://src.fedoraproject.org/lookaside/pkgs/keyutils/keyutils-1.5.8.tar.bz2/"
                "3c7f463039b83833c12a9414c2fcb389/keyutils-1.5.8.tar.bz2"
            ),
        ),
        md5="3c7f463039b83833c12a9414c2fcb389",
        note="MD5 published in Arch Linux's keyutils 1.5.8-1 PKGBUILD (archlinux/svntogit-packages "
        "a6cd0f050a660763a49e670a0886371bbd1178d3, 2013-10-28). No SHA-2 checksum was published for "
        "1.5.8: Fedora / CentOS 7 dist-git recorded MD5 / SHA-1, and Debian never packaged 1.5.8",
    ),
    ("keyutils", "1.5.10"): Upstream(
        "keyutils-1.5.10.tar.bz2",
        (
            f"{_LOOKASIDE}/keyutils/keyutils-1.5.10.tar.bz2/sha512/{_KEYUTILS_1510}/keyutils-1.5.10.tar.bz2",
            "https://people.redhat.com/~dhowells/keyutils/keyutils-1.5.10.tar.bz2",
        ),
        sha512=_KEYUTILS_1510,
    ),
    # FreeTDS, linked statically into pymssql's wheel (fetch_lgpl_sources.py
    # spec pymssql+freetds). The pin comes from Homebrew's formula at that
    # version, not from freetds.org, so the download host cannot vouch for itself.
    ("freetds", "1.5.19"): Upstream(
        "freetds-1.5.19.tar.bz2",
        ("https://www.freetds.org/files/stable/freetds-1.5.19.tar.bz2",),
        sha256="0dc2df2fea9934e3a99e00d417f3d192e9897572f6aff3905bd48f2507d16dff",
        note="SHA-256 from Homebrew's freetds formula at 1.5.19 (Homebrew/homebrew-core "
        "7cac0676d2fea7d4b783ce8b65e283b05cf37e58, 2026-08-16)",
    ),
    ("libxcrypt", "4.5.2"): Upstream(
        "libxcrypt-4.5.2.tar.xz",
        ("https://github.com/besser82/libxcrypt/releases/download/v4.5.2/libxcrypt-4.5.2.tar.xz",),
        sha256="71513a31c01a428bccd5367a32fd95f115d6dac50fb5b60c779d5c7942aec071",
    ),
    ("gcc", "8.5.0-el8"): Upstream(
        "gcc-8.5.0-20210514.tar.xz",
        (f"{_LOOKASIDE}/gcc/gcc-8.5.0-20210514.tar.xz/sha512/{_GCC_85_EL8}/gcc-8.5.0-20210514.tar.xz",),
        sha512=_GCC_85_EL8,
        # About 65 MB: in the companion source bundle, not the runtime image.
        bundle="companion",
    ),
}

#: Libraries whose binary carries no version: the reviewed version, keyed by
#: the bundled file's auditwheel name. Its hash suffix is derived from the
#: library's content, so a rebuilt library arrives under a new name and fails
#: closed ("review bundled libraries") whatever the wheel's own version is.
#: libcrypt-bc2db45e.so.1 (seen in psycopg2-binary 2.9.13) is libxcrypt
#: (glibc-compatible API: GLIBC_2.2.5 symbol versions plus libxcrypt's
#: ``crypt_gensalt`` family), built by the manylinux2014 image from source. It
#: was compiled by clang 22.1.8, i.e. a 2026 manylinux image, whose libxcrypt
#: pin is 4.5.2 (sha256 above, same as the manylinux pin).
WHEEL_LIB_VERSIONS: dict[str, str] = {
    "libcrypt-bc2db45e.so.1": "4.5.2",
}

#: The wheels whose ``.libs/`` were reviewed, at the reviewed version (the
#: inventory above). apps/api/constraints-image.txt pins the image's install to
#: these versions (tests/test_lgpl_source_shipping.py checks both agree), and a
#: bundling wheel at any other version fails the image build and the CI licence
#: gate with "review bundled libraries".
REVIEWED_WHEELS: dict[str, str] = {
    "google-crc32c": "1.9.0",
    "ijson": "3.6.0",
    "numpy": "2.5.4",
    "pillow": "13.0.0",
    "psycopg2-binary": "2.9.13",
    "pymssql": "2.4.6",
}

_REVIEW = "review bundled libraries"

_BASENAME = re.compile(r"^(?P<base>.+?)-[0-9a-f]{8}(?:-[0-9a-f]{8})?\.so(?:\.|$)")
_KEYUTILS = re.compile(rb"keyutils-(\d+\.\d+\.\d+)")
_EL_BUILD = re.compile(rb"\.so[.\d]*-(\d+\.\d+\.\d+)-[\w.]*?\.el(\d+)")


def lib_basename(filename: str) -> str:
    match = _BASENAME.match(filename)
    return match.group("base") if match else filename.split(".so", 1)[0]


def _canon(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def source_key(dist: str, dist_version: str, path: Path, lib: str, entry: Bundled) -> tuple[str, str]:
    data = path.read_bytes()
    if entry.family == "keyutils":
        match = _KEYUTILS.search(data)
        if match:
            return ("keyutils", match.group(1).decode())
    elif entry.family == "gcc":
        match = _EL_BUILD.search(data)
        if match:
            return ("gcc", f"{match.group(1).decode()}-el{match.group(2).decode()}")
    elif entry.family == "libxcrypt":
        if b"crypt_gensalt" not in data:
            raise BundledLibraryError(
                f"{dist} {dist_version}: {path.name} is not libxcrypt (no crypt_gensalt); review it"
            )
        version = WHEEL_LIB_VERSIONS.get(path.name)
        if version:
            return ("libxcrypt", version)
        raise BundledLibraryError(
            f"{_REVIEW}: {dist} {dist_version} bundles {path.name}, a {entry.component} build that was not "
            f"reviewed (reviewed: {', '.join(sorted(WHEEL_LIB_VERSIONS)) or 'none'}); identify its version "
            "and add the file name to WHEEL_LIB_VERSIONS in scripts/bundled_libraries.py"
        )
    raise BundledLibraryError(
        f"{_REVIEW}: {dist} {dist_version}: cannot tell which {entry.component} version {path.name} is; "
        "review the wheel and add it to WHEEL_LIB_VERSIONS / UPSTREAM in scripts/bundled_libraries.py"
    )


def inventory(dists: list[tuple[str, str, list[Path]]]) -> list[dict[str, Any]]:
    """One row per bundled library; raises on an unreviewed library or source."""
    rows: list[dict[str, Any]] = []
    problems: list[str] = []
    for dist, version, paths in dists:
        reviewed = REVIEWED_WHEELS.get(_canon(dist))
        if reviewed != version:
            problems.append(
                f"{_REVIEW}: {dist} {version} bundles native libraries and "
                + (f"was reviewed at {reviewed} only" if reviewed else "was never reviewed")
                + ": review its .libs/ and update REVIEWED_WHEELS (scripts/bundled_libraries.py) and "
                "apps/api/constraints-image.txt together"
            )
        for path in paths:
            lib = lib_basename(path.name)
            entry = BUNDLED.get(lib)
            if entry is None:
                problems.append(
                    f"{dist} {version} bundles {path.name} ({lib}), which is not reviewed: add its "
                    "licence to BUNDLED in scripts/bundled_libraries.py"
                )
                continue
            key = None
            if entry.copyleft:
                try:
                    key = source_key(dist, version, path, lib, entry)
                except BundledLibraryError as exc:
                    problems.append(str(exc))
                    continue
                if key not in UPSTREAM:
                    problems.append(
                        f"{_REVIEW}: {dist} {version}: no source mapped for {key[0]} {key[1]} ({path.name}); "
                        "add it to UPSTREAM in scripts/bundled_libraries.py"
                    )
                    continue
            rows.append({
                "dist": dist,
                "dist_version": version,
                "file": path.name,
                "library": lib,
                "component": entry.component,
                "licence": entry.licence,
                "copyleft": entry.copyleft,
                "attribution": entry.attribution,
                "source": list(key) if key else None,
                "source_location": source_location(key),
            })
    if problems:
        raise BundledLibraryError("; ".join(problems))
    return rows


def installed_bundles(distributions: Any = None) -> list[tuple[str, str, list[Path]]]:
    """(dist, version, bundled .so paths) for each installed distribution with a ``.libs`` dir."""
    from importlib import metadata

    out = []
    for dist in distributions if distributions is not None else metadata.distributions():
        if _canon(dist.metadata["Name"]) in OPERATOR_INSTALLED:
            continue
        paths = [
            Path(str(f.locate()))
            for f in dist.files or []
            if len(f.parts) == 2 and f.parts[0].endswith(".libs") and ".so" in f.name
        ]
        if paths:
            out.append((dist.metadata["Name"], dist.version, sorted(paths)))
    return out


# ── libraries linked statically into extension modules (best effort) ───────


@dataclass(frozen=True)
class StaticSignature:
    name: str  # stable key, used by STATIC_MAPPED
    component: str
    pattern: bytes  # first group, when present, is the version
    licence: str
    copyleft: bool = False
    attribution: str = ""


#: Version strings well-known libraries embed when linked statically. A
#: best-effort net: a library without such a string is not seen.
STATIC_SIGNATURES: tuple[StaticSignature, ...] = (
    StaticSignature("freetds", "FreeTDS", rb"freetds v(\d+\.\d+(?:\.\d+)?)", "LGPL-2.0-or-later", copyleft=True),
    StaticSignature("keyutils", "keyutils", rb"keyutils-(\d+\.\d+\.\d+)", "LGPL-2.1-or-later", copyleft=True),
    StaticSignature("readline", "GNU Readline", rb"GNU [Rr]eadline", "GPL-3.0-or-later", copyleft=True),
    StaticSignature("libgcrypt", "Libgcrypt", rb"Libgcrypt (\d+\.\d+\.\d+)", "LGPL-2.1-or-later", copyleft=True),
    StaticSignature("gnutls", "GnuTLS", rb"GnuTLS (\d+\.\d+\.\d+)", "LGPL-2.1-or-later", copyleft=True),
    StaticSignature("libiconv", "GNU libiconv", rb"GNU libiconv (\d+\.\d+)", "LGPL-2.1-or-later", copyleft=True),
    StaticSignature(
        "openssl", "OpenSSL", rb"OpenSSL (\d+\.\d+\.\d+[a-z]?)[ \x00]", _OPENSSL.licence,
        attribution=_OPENSSL.attribution,
    ),
    StaticSignature("boringssl", "BoringSSL", rb"BoringSSL", "ISC AND OpenSSL"),
    StaticSignature("zlib", "zlib", rb"(?:deflate|inflate) (\d+\.\d+\.\d+(?:\.\d+)?) Copyright", "Zlib"),
    StaticSignature(
        "sqlite", "SQLite", rb"(3\.\d+\.\d+) \d{4}-\d\d-\d\d \d\d:\d\d:\d\d [0-9a-f]{20}", "blessing (public domain)"
    ),
    StaticSignature("libpq", "PostgreSQL libpq", rb"PQconnectdbParams", "PostgreSQL"),
    StaticSignature("curl", "curl", rb"libcurl/(\d+\.\d+\.\d+)", "curl"),
)

#: Copyleft static libraries whose source the image ships, per wheel.
STATIC_MAPPED: dict[tuple[str, str], str] = {
    ("pymssql", "freetds"): "fetch_lgpl_sources.py spec pymssql+freetds",
}


def static_scan(dists: list[tuple[str, str, list[Path]]]) -> list[dict[str, Any]]:
    """Well-known libraries found inside extension modules; raises on an unmapped copyleft one."""
    rows: list[dict[str, Any]] = []
    problems: list[str] = []
    for dist, version, paths in dists:
        for path in paths:
            data = path.read_bytes()
            for sig in STATIC_SIGNATURES:
                match = re.search(sig.pattern, data)
                if not match:
                    continue
                source = STATIC_MAPPED.get((_canon(dist), sig.name)) if sig.copyleft else None
                if sig.copyleft and not source:
                    problems.append(
                        f"{dist} {version}: {path.name} links {sig.component} ({sig.licence}) statically, "
                        "and its source is not shipped: map it in STATIC_MAPPED and the Dockerfile step, "
                        "or review it"
                    )
                    continue
                rows.append({
                    "dist": dist,
                    "dist_version": version,
                    "file": path.name,
                    "library": sig.component,
                    "version": match.group(1).decode() if match.groups() else "",
                    "licence": sig.licence,
                    "copyleft": sig.copyleft,
                    "attribution": sig.attribution,
                    "source": source,
                })
    if problems:
        raise BundledLibraryError("; ".join(problems))
    return rows


def installed_extension_modules(distributions: Any = None) -> list[tuple[str, str, list[Path]]]:
    """(dist, version, native modules outside ``.libs``) for each shipped distribution."""
    from importlib import metadata

    out = []
    for dist in distributions if distributions is not None else metadata.distributions():
        if _canon(dist.metadata["Name"]) in OPERATOR_INSTALLED:
            continue
        paths = [
            Path(str(f.locate()))
            for f in dist.files or []
            if (f.name.endswith(".so") or ".so." in f.name)
            and not (len(f.parts) == 2 and f.parts[0].endswith(".libs"))
        ]
        if paths:
            out.append((dist.metadata["Name"], dist.version, sorted(paths)))
    return out
