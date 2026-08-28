"""OpenShift is a hosting plane, not a destination store.

Client requirement: move DynamoDB items onto a database that runs *on*
OpenShift (CloudNativePG, Crunchy PGO, MySQL operator, MongoDB operator).
The Kubernetes API is not a writer. Treating ``openshift`` as a dest
engine would invent a create-new into etcd / PVCs — forbidden.

This module is the SSOT for:

* mapping the catalog tile ``openshift`` onto a real dest driver
* resolving Service DNS (``svc.cluster.local``)
* stating what ``100%`` may mean (named fixture only)

Accuracy contract (honest vs Airbyte / Estuary / AWS DMS)
--------------------------------------------------------
A **consistent Scan snapshot** of DynamoDB items can be proven item-accurate
on a named fixture (HASH/RANGE, S/N/B/BOOL, SS/NS/BS, nested M/L, explicit
NULL vs missing) into PostgreSQL — the same wire as CNPG on OpenShift.

That is **not**:

* DynamoDB Streams CDC exactly-once (default remains at-least-once upsert)
* GSI / LSI / TTL / global-table replica copies
* attributes the application stored in S3 because of the 400 KB item cap
* an OpenShift cluster API migration
* AWS production live without credentials

``100%`` means every row on the named fixture. Never a platform-wide claim.
"""

from __future__ import annotations

from typing import Any

# Destinations that actually run as Services on OpenShift. Default is
# PostgreSQL — CloudNativePG and Crunchy PGO are the client-standard dest.
OPENSHIFT_STORE_DRIVERS: frozenset[str] = frozenset(
    {"postgresql", "mysql", "mongodb", "kafka", "redis"}
)
DEFAULT_OPENSHIFT_STORE = "postgresql"
DEFAULT_CLUSTER_DOMAIN = "svc.cluster.local"

# Tokens that mean "write to the OpenShift API" — always refuse.
_K8S_API_TOKENS = frozenset(
    {"openshift", "okd", "kubernetes", "k8s", "ocp"}
)


class OpenShiftDestError(ValueError):
    """Hosting profile cannot be resolved to a real destination store."""


def classify_openshift_store(store: str | None) -> str:
    """Real dest driver hosted on the cluster. Never ``openshift`` itself."""
    raw = str(store or DEFAULT_OPENSHIFT_STORE).strip().lower()
    if raw in _K8S_API_TOKENS:
        raise OpenShiftDestError(
            "OpenShift is the hosting plane, not a store. Choose PostgreSQL "
            "(CloudNativePG / Crunchy), MySQL, or MongoDB on the cluster."
        )
    if raw not in OPENSHIFT_STORE_DRIVERS:
        raise OpenShiftDestError(
            f"OpenShift cannot host dest driver {raw!r}. "
            f"Supported stores: {', '.join(sorted(OPENSHIFT_STORE_DRIVERS))}."
        )
    return raw


def resolve_openshift_service_host(
    *,
    service: str,
    namespace: str,
    cluster_domain: str = DEFAULT_CLUSTER_DOMAIN,
) -> str:
    """Cluster-local Service DNS — the dest host from inside the cluster."""
    svc = str(service or "").strip().rstrip(".")
    ns = str(namespace or "").strip().rstrip(".")
    domain = str(cluster_domain or DEFAULT_CLUSTER_DOMAIN).strip().strip(".")
    if not svc or not ns:
        raise OpenShiftDestError(
            "OpenShift dest needs Service name and namespace "
            "(e.g. orders-pg + payments → orders-pg.payments.svc.cluster.local)."
        )
    if "/" in svc or "/" in ns:
        raise OpenShiftDestError("Service and namespace must be DNS labels, not API paths.")
    return f"{svc}.{ns}.{domain}"


def apply_openshift_hosting(cfg: dict[str, Any]) -> dict[str, Any]:
    """Resolve OpenShift extras onto a dest cfg. Idempotent. Never writes k8s.

    Honours an already-set host (Route / NodePort / local port-forward) so a
    laptop Validate against ``127.0.0.1:5432`` is not overwritten by cluster DNS.
    When host is empty and Service+namespace are present, fill cluster DNS.
    """
    extra = cfg if isinstance(cfg, dict) else {}
    store = extra.get("openshift_store") or extra.get("store")
    service = str(extra.get("openshift_service") or extra.get("service") or "").strip()
    namespace = str(
        extra.get("openshift_namespace") or extra.get("namespace") or ""
    ).strip()
    domain = str(
        extra.get("openshift_cluster_domain") or extra.get("cluster_domain") or ""
    ).strip() or DEFAULT_CLUSTER_DOMAIN

    fmt = str(extra.get("type") or extra.get("format") or "").strip().lower()
    wants_openshift = fmt in _K8S_API_TOKENS or bool(service and namespace)
    if not wants_openshift:
        return cfg

    driver = classify_openshift_store(store)
    host = str(extra.get("host") or "").strip()
    if not host or host.lower() in _K8S_API_TOKENS:
        if service and namespace:
            host = resolve_openshift_service_host(
                service=service, namespace=namespace, cluster_domain=domain
            )
        else:
            raise OpenShiftDestError(
                "OpenShift dest needs a reachable PostgreSQL (or other store) "
                "host — Service+namespace, a Route, or a port-forward. "
                "The OpenShift API is not a destination."
            )
    out = dict(cfg)
    out["host"] = host
    out["type"] = driver
    out["openshift_store"] = driver
    out["openshift_hosting"] = True
    out["openshift_accuracy"] = accuracy_contract()
    if extra.get("port") in (None, "", 0) and driver == "postgresql":
        out["port"] = 5432
    return out


def accuracy_contract() -> dict[str, Any]:
    """What this route may claim. Named fixture only — never platform-wide."""
    return {
        "snapshot": (
            "DynamoDB Scan with ConsistentRead=true → dest upsert by HASH/RANGE. "
            "Item attributes on the named fixture are proven; GSI/LSI/TTL/Streams "
            "are not the snapshot."
        ),
        "cdc": "at-least-once upsert — DynamoDB Streams exactly-once is not claimed",
        "openshift": (
            "Hosting plane for PostgreSQL/MySQL/MongoDB. Not a dest engine. "
            "Not a Kubernetes API write."
        ),
        "one_hundred_percent": (
            "Measured on the named fixture only "
            "(apps/api/tests/test_dynamodb_openshift_fidelity_matrix.py)."
        ),
        "not_migrated": [
            "GSI / LSI copies",
            "TTL scheduler",
            "global-table replica topology",
            "S3 overflow attributes (>400 KB item pattern)",
            "application Query/GetItem rewrite",
        ],
    }
