"""SEC-9 x ERR: the local-file confinement refusal reaches the user in its own words.

The connection probe hands the refusal on as text, so the classifier must know
it by its words too, not only by the typed PathNotAllowedError. Before this,
/query/schemas answered "Datawrap couldn't identify the cause ... The database
said: SQLite file is outside the folders ..." (cut mid-sentence).
"""

from __future__ import annotations

from services.error_taxonomy import classify
from services.local_file_policy import confinement_refusal


def test_the_typed_refusal_keeps_its_words():
    named = classify(confinement_refusal("SQLite", "DATAFLOW_SQLITE_ROOT"), side="source", engine="sqlite")
    assert named.code == "access.path_not_allowed"


def test_the_refusal_as_text_is_known_by_its_words_and_kept_whole():
    from src.transfer.connector_registry import humanize_connection_error

    text = str(confinement_refusal("SQLite", "DATAFLOW_SQLITE_ROOT"))
    named = classify(RuntimeError(text), side="connection", engine="sqlite")
    assert named.code == "access.path_not_allowed", named
    copy = str(humanize_connection_error("sqlite", text))
    assert "couldn't identify the cause" not in copy
    assert "DATAFLOW_CONFINE_LOCAL_FILES=0" in copy and not copy.endswith("…"), copy
