"""M12 / X-138: the Mongo instant carrier label must not flip between the BSON
token ``date`` and ``TIMESTAMPTZ(6)``, and the document-store ``date`` must be
counted as a millisecond instant — not an unknown / calendar DATE.
"""

from services.carrier_instant import (
    carrier_rounded_columns,
    temporal_carrier_display_label,
)
from services.type_system import (
    destination_temporal_fractional_digits,
    temporal_precision_would_narrow,
)


def test_bson_date_is_a_millisecond_instant_not_unknown():
    assert destination_temporal_fractional_digits("date", dest_db="mongodb") == 3
    assert destination_temporal_fractional_digits("date", dest_db="elasticsearch") == 3
    # A real SQL DATE keeps zero fractional digits' worth of instant — the
    # instant-only branch must not leak onto relational engines.
    assert destination_temporal_fractional_digits("date", dest_db="postgresql") in (
        None,
        0,
    )


def test_verdict_label_names_the_bson_instant_consistently():
    label = temporal_carrier_display_label("date", engine="mongodb")
    assert "instant" in label.lower()
    assert "3" in label
    # A bare calendar DATE on a relational engine stays a DATE.
    assert temporal_carrier_display_label("date", engine="postgresql") == "date"
    assert temporal_carrier_display_label("TIMESTAMPTZ(6)", engine="mongodb") == "TIMESTAMPTZ(6)"


def test_rounded_note_labels_the_carrier_as_an_instant_not_a_date_flip():
    cols = carrier_rounded_columns(
        [
            {
                "source": "created_at",
                "target": "created_at",
                "source_type": "TIMESTAMPTZ(6)",
                "target_type": "date",
            }
        ],
        dest_engine="mongodb",
    )
    assert len(cols) == 1
    assert cols[0]["kept_fractional_digits"] == 3
    assert cols[0]["target_type"] != "date"
    assert "instant" in str(cols[0]["target_type"]).lower()
    assert cols[0]["source_type"] == "TIMESTAMPTZ(6)"


def test_us_source_into_bson_ms_is_a_declared_narrowing():
    assert temporal_precision_would_narrow(
        "TIMESTAMPTZ(6)", "date", dest_db="mongodb"
    ) is True
