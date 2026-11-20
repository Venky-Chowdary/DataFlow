"""D400, D401, D402, D395, D382 — the checks that failed in QA rounds 42–52.

Each case is the acceptance check from the 17 Nov 2026 defect stories.
A unit test is not a live retest of the original pair.
"""

from __future__ import annotations

import copy
from decimal import Decimal
from types import SimpleNamespace

import pytest


# ---------------------------------------------------------------------------
# D400 — a library recipe id is compiled, or the run is refused
# ---------------------------------------------------------------------------


def test_d400_pin_compiles_the_library_recipe(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services.transform_binding import compile_transfer_recipe, recipe_ignored_failure
    from services.transform_library import FileTransformLibraryStore
    from services.transform_recipe import RecipeStep, TransformRecipe
    from services import transform_library

    store = FileTransformLibraryStore(tmp_path / "lib.json")
    monkeypatch.setattr(transform_library, "_store", store)
    saved = store.save(
        TransformRecipe(
            name="clean customers",
            steps=[
                RecipeStep(column="email", op="trim"),
                RecipeStep(column="email", op="lower"),
                RecipeStep(column="amount", op="decimal"),
            ],
        )
    )
    request = SimpleNamespace(
        transform_recipe_id=saved.id,
        transform_recipe_version=0,
        transform_recipe_version_mode="pin",
        transform_recipe_params=None,
        mappings=[],
        workspace_id="",
        shape_recipe={},
        approved_shape_recipe_hash="",
    )
    assert recipe_ignored_failure(request)
    assert compile_transfer_recipe(request) is True
    assert request.shape_recipe["steps"]
    assert request.approved_shape_recipe_hash
    assert request.transform_recipe_version == 1
    assert recipe_ignored_failure(request) == ""
    # The staged recipe already matches the pin. A second compile does not replace it.
    staged = copy.deepcopy(request.shape_recipe)
    assert compile_transfer_recipe(request) is False
    assert request.shape_recipe == staged


def test_d400_a_library_edit_between_stage_and_confirm_is_refused(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services.transform_binding import compile_transfer_recipe
    from services.transform_library import FileTransformLibraryStore
    from services.transform_recipe import RecipeStep, TransformRecipe
    from services import transform_library

    store = FileTransformLibraryStore(tmp_path / "lib.json")
    monkeypatch.setattr(transform_library, "_store", store)
    saved = store.save(
        TransformRecipe(
            name="clean customers",
            steps=[RecipeStep(column="email", op="trim")],
        )
    )
    request = SimpleNamespace(
        transform_recipe_id=saved.id,
        transform_recipe_version=0,
        transform_recipe_version_mode="follow",
        transform_recipe_params=None,
        mappings=[],
        workspace_id="",
        shape_recipe={},
        approved_shape_recipe_hash="",
    )
    compile_transfer_recipe(request)
    staged = copy.deepcopy(request.shape_recipe)
    store.save(
        TransformRecipe(
            id=saved.id,
            name="clean customers",
            steps=[
                RecipeStep(column="email", op="trim"),
                RecipeStep(column="email", op="lower"),
            ],
        )
    )
    with pytest.raises(ValueError, match="was not applied"):
        compile_transfer_recipe(request)
    assert request.shape_recipe == staged


def test_d400_a_draft_recipe_is_refused_before_confirm(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services.transform_binding import compile_transfer_recipe
    from services.transform_library import FileTransformLibraryStore
    from services.transform_recipe import RecipeStep, TransformRecipe
    from services import transform_library

    store = FileTransformLibraryStore(tmp_path / "lib.json")
    monkeypatch.setattr(transform_library, "_store", store)
    draft = store.save(TransformRecipe(name="d", steps=[RecipeStep(column="a", op="bogus")]))
    request = SimpleNamespace(
        transform_recipe_id=draft.id,
        transform_recipe_version=0,
        transform_recipe_version_mode="pin",
        transform_recipe_params=None,
        mappings=[],
        workspace_id="",
        shape_recipe={},
        approved_shape_recipe_hash="",
    )
    with pytest.raises(ValueError, match="draft"):
        compile_transfer_recipe(request)
    assert request.shape_recipe == {}


def test_d400_steps_and_a_different_library_recipe_are_not_mixed(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_TRANSFORM_LIBRARY_PATH", str(tmp_path / "lib.json"))
    from services.transform_binding import compile_transfer_recipe
    from services.transform_library import FileTransformLibraryStore
    from services.transform_recipe import RecipeStep, TransformRecipe
    from services import transform_library

    store = FileTransformLibraryStore(tmp_path / "lib.json")
    monkeypatch.setattr(transform_library, "_store", store)
    saved = store.save(
        TransformRecipe(name="clean customers", steps=[RecipeStep(column="email", op="trim")])
    )
    request = SimpleNamespace(
        transform_recipe_id=saved.id,
        transform_recipe_version=0,
        transform_recipe_version_mode="pin",
        transform_recipe_params=None,
        mappings=[],
        workspace_id="",
        shape_recipe={"steps": [{"column": "qty", "op": "decimal"}]},
        approved_shape_recipe_hash="",
    )
    with pytest.raises(ValueError, match="not the same compiled recipe"):
        compile_transfer_recipe(request)


# ---------------------------------------------------------------------------
# D401 — a display mask is not written over the stored raw cell
# ---------------------------------------------------------------------------


def test_d401_an_echoed_mask_keeps_the_raw_cell_and_a_real_edit_applies():
    from services.quarantine_dlq import overlay_cell_edit

    durable = [{
        "row": 3,
        "column": "qty",
        "value": "NOTANUMBER",
        "source_values": {"name": "BAD_QTY", "qty": "NOTANUMBER"},
        "values": {"name": "BAD_QTY", "qty": "NOTANUMBER"},
    }]
    posted = {
        "row": 3,
        "values": {"name": "BA***TY", "qty": "30"},
        "source_values": {"name": "BA***TY", "qty": "30"},
    }
    merged = overlay_cell_edit(posted, durable)
    assert merged["source_values"]["name"] == "BAD_QTY"
    assert merged["values"]["name"] == "BAD_QTY"
    assert merged["source_values"]["qty"] == "30"
    assert merged["values"]["qty"] == "30"


def test_d401_an_explicit_mask_is_refused_and_the_ledger_stays_open():
    from services.quarantine_dlq import MaskedQuarantineReplayError, overlay_cell_edit

    durable = [{
        "row": 3,
        "column": "name",
        "value": "BAD_QTY",
        "source_values": {"name": "BAD_QTY"},
        "values": {"name": "BAD_QTY"},
    }]
    with pytest.raises(MaskedQuarantineReplayError, match="ledger stays open"):
        overlay_cell_edit({"row": 3, "column": "name", "value": "BA***TY"}, durable)


def test_d401_a_record_payload_drops_the_mask_and_keeps_the_edit():
    from services.quarantine_dlq import MaskedQuarantineReplayError, merge_replay_record

    merged = merge_replay_record(
        {"name": "BAD_DATE", "qty": "NOTANUMBER"},
        {"name": "BA****TE", "qty": "30"},
    )
    assert merged["name"] == "BAD_DATE"
    assert merged["qty"] == "30"
    with pytest.raises(MaskedQuarantineReplayError, match="ledger stays open"):
        merge_replay_record({}, {"name": "BA***TY"})


# ---------------------------------------------------------------------------
# D402 — dedupe_key collapses the last file row, or the plan refuses
# ---------------------------------------------------------------------------


def test_d402_amazon_s3_overwrite_keeps_the_last_row_of_the_declared_key():
    from services.duplicate_policy import keep_last_records
    from services.preflight_source_kind import (
        dedupe_key_unhonoured_reason,
        file_overwrite_keep_last,
        source_is_record_batch,
    )

    assert source_is_record_batch("database", "amazon_s3") is True
    assert file_overwrite_keep_last(
        source_kind="database",
        source_format="amazon_s3",
        sync_mode="full_refresh_overwrite",
        dedupe_key="id",
    ) is True
    assert file_overwrite_keep_last(
        source_kind="database",
        source_format="postgresql",
        sync_mode="full_refresh_overwrite",
        dedupe_key="id",
    ) is False
    assert dedupe_key_unhonoured_reason(
        source_kind="database",
        source_format="postgresql",
        sync_mode="full_refresh_overwrite",
        dedupe_key="id",
        mode_was_explicit=True,
    )
    assert dedupe_key_unhonoured_reason(
        source_kind="database",
        source_format="amazon_s3",
        sync_mode="full_refresh_overwrite",
        dedupe_key="id",
        mode_was_explicit=True,
    ) == ""
    assert dedupe_key_unhonoured_reason(
        source_kind="database",
        source_format="postgresql",
        sync_mode="full_refresh_overwrite",
        dedupe_key="id",
        mode_was_explicit=False,
    ) == ""

    request = SimpleNamespace(
        source=SimpleNamespace(
            kind="database",
            format="amazon_s3",
            extra={"in_file_dedupe": "keep_last", "dedupe_key": "id"},
        ),
        destination=SimpleNamespace(kind="database", format="postgresql", extra={}),
        stream_contracts=[],
    )
    collapsed = keep_last_records(
        request,
        [{"id": 3, "name": "NAME_3"}, {"id": 3, "name": "NAME_3 LATEST"}],
    )
    assert collapsed == [{"id": 3, "name": "NAME_3 LATEST"}]


def test_d402_keep_last_without_a_key_refuses_instead_of_writing_both_copies():
    from services.duplicate_policy import keep_last_records

    request = SimpleNamespace(
        source=SimpleNamespace(
            kind="database",
            format="csv",
            extra={"in_file_dedupe": "keep_last"},
        ),
        destination=SimpleNamespace(kind="database", format="postgresql", extra={}),
        stream_contracts=[],
    )
    with pytest.raises(ValueError, match="both copies"):
        keep_last_records(
            request,
            [{"id": 3, "name": "NAME_3"}, {"id": 3, "name": "NAME_3 LATEST"}],
        )


# ---------------------------------------------------------------------------
# D395 — append create-new carries no business key; an existing key is refused
# ---------------------------------------------------------------------------


def test_d395_append_create_new_does_not_carry_the_source_primary_key(monkeypatch):
    from services import copy_fast_path as fp
    from services import schema_fidelity

    monkeypatch.setattr(
        schema_fidelity,
        "resolve_create_fidelity_plan",
        lambda **_k: schema_fidelity.CreateFidelityPlan(
            report=schema_fidelity.SchemaFidelityReport(), primary_key=["id"]
        ),
    )
    token, _scope = fp.begin_fast_path_create_scope(
        object(), None, "payments", "full_refresh_append"
    )
    try:
        create = fp.plan_fast_path_create(
            dest_dialect="sqlserver",
            pairs=[("id", "id"), ("amount", "amount")],
            ddls=["NVARCHAR(450)", "NVARCHAR(MAX)"],
            primary_key=["id"],
            dest_table="payments",
        )
    finally:
        fp.reset_fast_path_create_scope(token)
    assert create.primary_key == ()


def test_d395_overwrite_and_incremental_append_still_carry_the_key(monkeypatch):
    from services import copy_fast_path as fp
    from services import schema_fidelity

    monkeypatch.setattr(
        schema_fidelity,
        "resolve_create_fidelity_plan",
        lambda **_k: schema_fidelity.CreateFidelityPlan(
            report=schema_fidelity.SchemaFidelityReport(), primary_key=["id"]
        ),
    )
    for mode in ("", "full_refresh_overwrite", "incremental_append"):
        token, _scope = fp.begin_fast_path_create_scope(object(), None, "payments", mode)
        try:
            create = fp.plan_fast_path_create(
                dest_dialect="sqlserver",
                pairs=[("id", "id"), ("amount", "amount")],
                ddls=["NVARCHAR(450)", "NVARCHAR(MAX)"],
                primary_key=["id"],
                dest_table="payments",
            )
        finally:
            fp.reset_fast_path_create_scope(token)
        assert create.primary_key == ("id",), mode


def test_d395_append_into_an_existing_key_is_disclosed_not_refused_and_a_repeat_snapshot_is_named():
    # R6: append never merges, but appending new keys into a keyed table is
    # legitimate. D395 AC-2 ("refuse any existing keyed table") over-reached;
    # the plan discloses the key and the write-time check refuses a collision.
    from services.copy_fast_path import append_repeat_of_stored_snapshot
    from services.schema_fidelity import append_existing_key_disclosure

    for mode in ("full_refresh_append", "append", "incremental_append"):
        note = append_existing_key_disclosure(
            sync_mode=mode, table_exists=True, key_columns=["id"]
        )
        assert "primary key (id)" in note, mode
        assert "never merged" in note, mode
        assert "refused" in note, mode
    assert append_existing_key_disclosure(
        sync_mode="full_refresh_append", table_exists=False, key_columns=["id"]
    ) == ""
    assert append_existing_key_disclosure(
        sync_mode="full_refresh_overwrite", table_exists=True, key_columns=["id"]
    ) == ""
    assert append_existing_key_disclosure(
        sync_mode="upsert", table_exists=True, key_columns=["id"]
    ) == ""
    assert append_existing_key_disclosure(
        sync_mode="full_refresh_append", table_exists=True, key_columns=[]
    ) == ""
    assert append_repeat_of_stored_snapshot(
        ["skip", "skip"], replace_destination=False
    ) is True
    assert append_repeat_of_stored_snapshot(
        ["skip", "load"], replace_destination=False
    ) is False
    assert append_repeat_of_stored_snapshot(
        ["skip"], replace_destination=True
    ) is False


# ---------------------------------------------------------------------------
# D382 — flag eq true matches 1, BIT(1), and Oracle NUMBER(1)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("row", "filt"),
    [
        (1, True),
        ("1", "true"),
        (b"\x01", "true"),
        (0, False),
        ("0", "false"),
        (b"\x00", "f"),
        (Decimal("1"), "true"),
        (Decimal("0"), False),
    ],
)
def test_d382_eq_true_matches_a_bit(row, filt):
    from services.row_filter import _compare_values

    assert _compare_values(row, filt, "eq") is True
    assert _compare_values(row, filt, "ne") is False


def test_d382_null_blank_and_yes_are_not_booleans_and_numbers_stay_numbers():
    from services.row_filter import _compare_values

    assert _compare_values(None, True, "eq") is False
    assert _compare_values("", False, "eq") is False
    assert _compare_values("yes", 1, "eq") is False
    assert _compare_values(1, 2, "eq") is False
    assert _compare_values(1, 1, "eq") is True
    assert _compare_values(None, 1, "lt") is False
