"""A zoneless source column, and the fact only the operator has.

A Postgres ``timestamp`` records wall-clock digits and no zone. Writing it to a
carrier that stores instants — BSON ``date``, ``TIMESTAMPTZ`` — has to pick one,
and picking UTC silently is how a business day moves by hours for anyone whose
data was never UTC. Blocking is right.

But a block is only half an answer. The remediation used to say "map to a
wall-clock destination carrier", which MongoDB does not have: BSON stores
instants and nothing else. That named an exit that could not be taken.

The missing input is the zone itself, which the source never recorded and the
tool cannot infer. ``assume_timezone:<zone>`` is the operator supplying it, and
from that point the column genuinely carries an instant — so the verdict has to
move with the value, or the declaration changes what is written without
unblocking the transfer.
"""

from __future__ import annotations

import pytest

from services.timezone_policy import (
    POLICY_UTC_INVENT,
    effective_source_type,
    resolve_timezone_policy,
)
from services.transform_engine import apply_transform

NAIVE = "2024-01-05 10:30:00"


class TestDeclaringTheZone:
    def test_a_declared_zone_produces_a_real_instant(self):
        value, err = apply_transform(NAIVE, "assume_timezone:Europe/Berlin")
        assert err is None
        assert value == "2024-01-05T10:30:00+01:00"

    def test_the_zone_is_a_zone_not_a_fixed_offset(self):
        """Berlin is +01:00 in January and +02:00 in July.

        A fixed-offset shortcut would answer the same for both and be wrong for
        half the year.
        """
        winter, _ = apply_transform("2024-01-15 12:00:00", "assume_timezone:Europe/Berlin")
        summer, _ = apply_transform("2024-07-15 12:00:00", "assume_timezone:Europe/Berlin")
        assert winter.endswith("+01:00")
        assert summer.endswith("+02:00")

    def test_a_value_that_already_states_an_offset_is_left_alone(self):
        """The source was explicit; a declaration must not move it."""
        value, err = apply_transform("2024-01-05T10:30:00+05:00", "assume_timezone:UTC")
        assert err is None
        assert value == "2024-01-05T10:30:00+05:00"

    @pytest.mark.parametrize(
        "transform, fragment",
        [
            ("assume_timezone:Nowhere/Bad", "Unknown timezone"),
            ("assume_timezone:", "needs a zone"),
        ],
    )
    def test_an_unusable_declaration_is_refused_not_guessed(self, transform, fragment):
        value, err = apply_transform(NAIVE, transform)
        assert value is None
        assert fragment in err


class TestTheVerdictMovesWithTheValue:
    def test_an_undeclared_zoneless_source_still_requires_a_contract(self):
        policy = resolve_timezone_policy("TIMESTAMP", "date", dest_db="mongodb")
        assert policy is not None
        assert policy.policy == POLICY_UTC_INVENT
        assert policy.requires_contract
        assert not policy.instant_preserved

    def test_declaring_the_zone_clears_the_block(self):
        declared = effective_source_type("TIMESTAMP", "assume_timezone:Europe/Berlin")
        policy = resolve_timezone_policy(declared, "date", dest_db="mongodb")
        assert policy is not None
        assert not policy.requires_contract
        assert policy.instant_preserved

    def test_a_source_that_already_had_a_zone_is_unchanged_by_a_declaration(self):
        assert effective_source_type("TIMESTAMPTZ", "assume_timezone:UTC") == "TIMESTAMPTZ"

    def test_an_empty_or_absent_declaration_changes_nothing(self):
        assert effective_source_type("TIMESTAMP", None) == "TIMESTAMP"
        assert effective_source_type("TIMESTAMP", "assume_timezone:") == "TIMESTAMP"
        assert effective_source_type("TIMESTAMP", "datetime") == "TIMESTAMP"


class TestTheRemediationNamesOnlyRealExits:
    def test_mongo_is_not_told_to_use_a_carrier_bson_does_not_have(self):
        policy = resolve_timezone_policy("TIMESTAMP", "date", dest_db="mongodb")
        assert "assume_timezone" in policy.remediation
        assert "wall-clock carrier to map to" in policy.remediation
        assert "or map to a wall-clock destination carrier" not in policy.remediation

    def test_a_sql_destination_keeps_the_carrier_option_it_does_have(self):
        policy = resolve_timezone_policy("TIMESTAMP", "TIMESTAMPTZ", dest_db="postgresql")
        assert "assume_timezone" in policy.remediation
        assert "wall-clock destination carrier" in policy.remediation


class TestTheDocumentInstantIsClassifiedAtAll:
    def test_bson_date_is_read_as_an_instant_carrier(self):
        """It shares a name with SQL DATE and behaves nothing like it.

        While the token was unclassified the timezone question was never asked
        on the route where it matters most.
        """
        policy = resolve_timezone_policy("TIMESTAMPTZ", "date", dest_db="mongodb")
        assert policy is not None
        assert policy.instant_preserved

    def test_a_sql_date_target_is_not_dragged_along(self):
        policy = resolve_timezone_policy("TIMESTAMPTZ", "DATE", dest_db="postgresql")
        assert policy is None or not policy.instant_preserved


def test_the_write_path_honours_the_declaration():
    """resolve_transform must keep it, or the value is written zoneless anyway."""
    from services.transform_resolver import resolve_transform

    mapping = {
        "source": "created_at",
        "target": "created_at",
        "transform": "assume_timezone:UTC",
        "target_type": "TIMESTAMP",
    }
    assert resolve_transform(
        mapping, column_types={"created_at": "TIMESTAMP"}
    ) == "assume_timezone:UTC"
