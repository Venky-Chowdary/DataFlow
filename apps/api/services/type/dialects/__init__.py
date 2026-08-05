"""Per-database type dialect mappers.

Each module is responsible for converting native type strings from one source
into Datawrap logical carriers. No connector-specific logic should live outside
these modules.
"""

from __future__ import annotations
