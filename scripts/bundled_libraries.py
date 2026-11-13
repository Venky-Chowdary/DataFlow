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
    note: str = ""


_LOOKASIDE = "https://sources.stream.centos.org/sources/rpms"
_KEYUTILS_1510 = (
    "7f6f956c7e76cdc2aeb52e74fe670b20a5f9a5d9b543fd2ce971d80c48745f37"
    "d05235a42f0a8f152b1128a109c7d8bf07e751282a20d2d3f433a99a5308ae8d"
)
_GCC_85_EL8 = (
    "4811dbcbdce8fe0fa8bc1fdca024988a1a75ac89e3efc675fe991b81032c220a"
    "b4bd1bc530bb99f6209ffeca67581a198de82f7253530de8c24a2a81770eb4ea"
)

#: (family, version) -> where its source comes from. Checksums: CentOS Stream
#: dist-git ``sources`` files (keyutils c8s, gcc c8s) and the manylinux
#: libxcrypt pin, all read on 2026-11-13.
UPSTREAM: dict[tuple[str, str], Upstream] = {
    ("keyutils", "1.5.8"): Upstream(
        "keyutils-1.5.8.tar.bz2",
        ("https://people.redhat.com/~dhowells/keyutils/keyutils-1.5.8.tar.bz2",),
        note="no published checksum found for 1.5.8 (CentOS 7 era); recorded at download",
    ),
    ("keyutils", "1.5.10"): Upstream(
        "keyutils-1.5.10.tar.bz2",
        (
            f"{_LOOKASIDE}/keyutils/keyutils-1.5.10.tar.bz2/sha512/{_KEYUTILS_1510}/keyutils-1.5.10.tar.bz2",
            "https://people.redhat.com/~dhowells/keyutils/keyutils-1.5.10.tar.bz2",
        ),
        sha512=_KEYUTILS_1510,
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
    ),
}

#: Libraries whose binary carries no version: the reviewed version per wheel.
#: psycopg2-binary's manylinux2014 wheels link libcrypt.so.1 from libxcrypt,
#: which the manylinux2014 image builds from source (glibc-compatible API:
#: GLIBC_2.2.5 symbol versions plus libxcrypt's ``crypt_gensalt`` family). The
#: wheel's copy was compiled by clang 22.1.8, i.e. a 2026 manylinux image, whose
#: libxcrypt pin is 4.5.2 (sha256 above, same as the manylinux pin).
WHEEL_LIB_VERSIONS: dict[tuple[str, str, str], str] = {
    ("psycopg2-binary", "2.9.13", "libcrypt"): "4.5.2",
}

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
        version = WHEEL_LIB_VERSIONS.get((_canon(dist), dist_version, lib))
        if version:
            return ("libxcrypt", version)
    raise BundledLibraryError(
        f"{dist} {dist_version}: cannot tell which {entry.component} version {path.name} is; "
        "review the wheel and add it to WHEEL_LIB_VERSIONS / UPSTREAM in scripts/bundled_libraries.py"
    )


def inventory(dists: list[tuple[str, str, list[Path]]]) -> list[dict[str, Any]]:
    """One row per bundled library; raises on an unreviewed library or source."""
    rows: list[dict[str, Any]] = []
    problems: list[str] = []
    for dist, version, paths in dists:
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
                    problems.append(f"{dist} {version}: no source mapped for {key[0]} {key[1]} ({path.name})")
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
