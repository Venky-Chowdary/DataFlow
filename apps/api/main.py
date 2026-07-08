"""
Backward-compatible API entrypoint.

Development and production should use the modular application:

    uvicorn src.main:app --reload --port 8001

This module re-exports the canonical app so older scripts that reference
`main:app` continue to work without duplicating route definitions.
"""

from src.main import app

__all__ = ["app"]
