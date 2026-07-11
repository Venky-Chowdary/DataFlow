"""Universal transfer package.

Keep the package import-light so capability-only consumers (for example the
connector catalog and tests) can import `transfer.connector_capabilities`
without triggering the full engine dependency chain.
"""

from __future__ import annotations

__all__ = [
    "UniversalTransferEngine",
    "get_transfer_engine",
    "TransferCapabilities",
    "TransferRequest",
    "TransferResult",
    "get_capabilities",
    "validate_transfer",
]


def __getattr__(name: str):
    if name == "UniversalTransferEngine":
        from .engine import UniversalTransferEngine
        return UniversalTransferEngine
    if name == "get_transfer_engine":
        from .engine import get_transfer_engine
        return get_transfer_engine
    if name == "TransferCapabilities":
        from .models import TransferCapabilities
        return TransferCapabilities
    if name == "TransferRequest":
        from .models import TransferRequest
        return TransferRequest
    if name == "TransferResult":
        from .models import TransferResult
        return TransferResult
    if name == "get_capabilities":
        from .registry import get_capabilities
        return get_capabilities
    if name == "validate_transfer":
        from .registry import validate_transfer
        return validate_transfer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
