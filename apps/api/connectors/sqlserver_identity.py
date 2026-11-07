"""Studio import path for SQL Server IDENTITY_INSERT.

The implementation lives in ``connectors.identity_insert`` so ``writer_common``
stays inside its line budget and tests can patch
``connectors.identity_insert``. This module keeps the studio import path on
the same functions.
"""

from connectors.identity_insert import (
    IdentityInsertSession,
    begin_identity_insert,
    sqlserver_identity_columns,
)

__all__ = [
    "IdentityInsertSession",
    "begin_identity_insert",
    "sqlserver_identity_columns",
]
