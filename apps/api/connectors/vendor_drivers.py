"""Database drivers the operator installs, not Datawrap (owner decision, 2026-11-13).

SAP's ``hdbcli``, Teradata's ``teradatasql`` and IBM's ``ibm_db`` (which
bundles IBM's proprietary Db2 ``clidriver``) are proprietary. Their vendor
licences do not clearly allow Datawrap to redistribute them inside its image,
so the image does not ship them, nor the SQLAlchemy dialects the generic SQL
path dials through (``sqlalchemy-hana``, ``teradatasqlalchemy``,
``ibm-db-sa``). An operator
who has accepted the vendor's licence installs them on the API host
(``pip install ...``, or the API image's ``INSTALL_VENDOR_DRIVERS=1``
build-arg).

Without them, every surface (connector Test, save, run, the catalog) gives
the same answer: which packages to install, and that the operator must accept
the vendor's licence. No stack trace, no file path, no connection attempt.
Standard library only, so the catalog can ask cheaply.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from typing import Any

#: Docker build-arg that installs the vendor drivers into the API image.
BUILD_ARG = "INSTALL_VENDOR_DRIVERS"
#: Catalog label while the driver is missing.
REQUIRES_OPERATOR_DRIVER = "Requires operator-installed driver"


@dataclass(frozen=True)
class VendorDriver:
    label: str
    vendor: str
    licence: str
    packages: tuple[str, ...]  # pip names
    modules: tuple[str, ...]  # import names, same order


SAP_HANA = VendorDriver(
    label="SAP HANA",
    vendor="SAP",
    licence="SAP's licence for the SAP HANA client (hdbcli)",
    packages=("hdbcli", "sqlalchemy-hana"),
    modules=("hdbcli", "sqlalchemy_hana"),
)
TERADATA = VendorDriver(
    label="Teradata",
    vendor="Teradata",
    licence="Teradata's licence for its SQL driver (teradatasql)",
    packages=("teradatasql", "teradatasqlalchemy"),
    modules=("teradatasql", "teradatasqlalchemy"),
)

IBM_DB2 = VendorDriver(
    label="IBM Db2",
    vendor="IBM",
    licence="IBM's licence for the Db2 client (ibm_db and the clidriver it bundles)",
    packages=("ibm-db", "ibm-db-sa"),
    modules=("ibm_db", "ibm_db_sa"),
)

#: Catalog ids, connector types and SQLAlchemy driver names -> driver.
_BY_KIND: dict[str, VendorDriver] = {
    "sap_hana": SAP_HANA,
    "hana": SAP_HANA,
    "sap_bw_4hana": SAP_HANA,
    "teradata": TERADATA,
    "teradata_vantage": TERADATA,
    "teradatasql": TERADATA,
    "db2": IBM_DB2,
    "ibm_db2": IBM_DB2,
    "ibm_db2_warehouse": IBM_DB2,
    "ibm_db_sa": IBM_DB2,
}


def vendor_driver(kind: str | None) -> VendorDriver | None:
    return _BY_KIND.get(str(kind or "").strip().lower())


def _installed(module: str) -> bool:
    if module in sys.modules:
        return sys.modules[module] is not None
    try:
        return importlib.util.find_spec(module) is not None
    except (ImportError, ValueError):
        return False


def missing_packages(driver: VendorDriver) -> list[str]:
    return [pkg for pkg, mod in zip(driver.packages, driver.modules) if not _installed(mod)]


def operator_driver_message(kind: str | None) -> str:
    """The refusal for a vendor connector whose driver is missing, else ``""``."""
    driver = vendor_driver(kind)
    if driver is None or not missing_packages(driver):
        return ""
    return (
        f"Not available on this server: {driver.label} needs a driver the operator "
        f"installs. Datawrap does not ship it because it is licensed by "
        f"{driver.vendor}. An operator who has accepted {driver.licence} installs it "
        f"on the Datawrap API host with `pip install {' '.join(driver.packages)}` "
        f"(or builds the API image with `--build-arg {BUILD_ARG}=1`) and restarts "
        "the API. No connection was attempted."
    )


def operator_driver_info(kind: str | None) -> dict[str, Any] | None:
    """Catalog facts for a vendor connector, ``None`` for every other one."""
    driver = vendor_driver(kind)
    if driver is None:
        return None
    missing = missing_packages(driver)
    return {
        "vendor": driver.vendor,
        "packages": list(driver.packages),
        "installed": not missing,
        "missing": missing,
        "build_arg": f"{BUILD_ARG}=1",
        "note": REQUIRES_OPERATOR_DRIVER,
    }
