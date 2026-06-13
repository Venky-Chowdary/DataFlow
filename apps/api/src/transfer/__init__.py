"""Universal transfer engine — any source to any destination."""

from .engine import UniversalTransferEngine, get_transfer_engine
from .models import TransferCapabilities, TransferRequest, TransferResult
from .registry import get_capabilities, validate_transfer

__all__ = [
    "UniversalTransferEngine",
    "get_transfer_engine",
    "TransferCapabilities",
    "TransferRequest",
    "TransferResult",
    "get_capabilities",
    "validate_transfer",
]
