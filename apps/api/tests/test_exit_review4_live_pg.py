"""Exit rehearsal on live PostgreSQL: the false "proven" shapes review 4 found (C-1, H-2).

psycopg2 reads some types into a Python value that is not one-to-one with what
the server stores: an array's lower bound is dropped, ``24:00:00`` reads as
``00:00:00``, an interval reads as a plain duration and a jsonb number loses
its scale (``1.50`` reads as ``1.5``). The extract writer is fed the same
folded value, so comparing the driver's value with the extract proved
nothing. The source side now reads the server's own text (``col::text``) for
those types: a value the driver folds is named (``not_proven``, the column and
the reason in the table's reason), and a value the extract changed while the
driver read it faithfully differs (``incomplete``). Skipped when no
PostgreSQL is reachable.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import pytest

from tests.test_exit_rehearsal_e2e import world  # noqa: F401
from tests.test_exit_review3_live_pg import _rehearse, pg  # noqa: F401

#: table -> (columns, rows, what the not_proven reason must say). Never proven, in JSONL or CSV.
NAMED = {
    "arrlb": ("id BIGINT PRIMARY KEY, a INT[]", ["1, '[2:3]={1,2}'", "2, '{1,2}'"],
              "a holds an array whose lower bound is not 1"),
    "tm24": ("id BIGINT PRIMARY KEY, t TIME", ["1, '24:00:00'", "2, '00:00:00'"],
             "t holds 24:00:00, which the driver reads as 00:00:00"),
    "ttz24": ("id BIGINT PRIMARY KEY, t TIMETZ", ["1, '24:00:00+00'", "2, '00:00:00+00'"],
              "t holds 24:00:00, which the driver reads as 00:00:00"),
    "jbnum": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, '[1.0, 1.00]'", "2, '{\"p\": 1.5}'"],
              "jb holds a JSON number whose scale the driver drops"),
    "jbnest": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, '{\"q\": [2.000]}'", "2, '{\"q\": [2]}'"],
               "jb holds a JSON number whose scale the driver drops"),
    "jbtop": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, '1.10'", "2, '1.1'"],
              "jb holds a JSON number whose scale the driver drops"),
    "ivday": ("id BIGINT PRIMARY KEY, iv INTERVAL", ["1, '24:00:00'", "2, '1 day'"],
              "iv holds an interval the driver reads as a plain duration"),
    "jbarr": ("id BIGINT PRIMARY KEY, a JSONB[]", ["1, ARRAY['{\"p\": 1.50}'::jsonb]"],
              "a holds an array element the driver reads as another value"),
}
#: table -> (columns, rows, the column the difference must name). The driver read the value faithfully,
#: the extract did not keep it: never proven, the column named.
DIFFERS = {
    "numarr": ("id BIGINT PRIMARY KEY, a NUMERIC[]", ["1, '{1.50,2}'", "2, '{1.5,2}'"], "a"),
}
#: Shapes the driver reads one-to-one: still proven in both formats.
HONEST = {
    "arr1": ("id BIGINT PRIMARY KEY, a INT[], m INT[], e INT[]",
             ["1, '{1,2}', '{{1,2},{3,4}}', '{}'", "2, NULL, '{{5,6}}', '{}'"]),
    "numarr_plain": ("id BIGINT PRIMARY KEY, a NUMERIC[]", ["1, '{1.5,2}'", "2, '{-3,0.25}'"]),
    "f8arr": ("id BIGINT PRIMARY KEY, a FLOAT8[]", ["1, '{0.1,-0}'", "2, '{0.1,0}'"]),
    "tarr": ("id BIGINT PRIMARY KEY, a TEXT[], b BOOL[]", ["1, ARRAY['NULL', 'a,b'], '{t,f}'",
                                                             "2, ARRAY[NULL]::text[], '{f}'"]),
    "times": ("id BIGINT PRIMARY KEY, t TIME, z TIMETZ", ["1, '00:00:00', '10:00:00+02'",
                                                          "2, '23:59:59.5', '23:59:59.5-05:30'"]),
    "ivok": ("id BIGINT PRIMARY KEY, iv INTERVAL", ["1, '1 day'", "2, '3 days'"]),
    "jbok": ("id BIGINT PRIMARY KEY, jb JSONB", ["1, '{\"p\": 1.5, \"q\": [2, 1.0]}'", "2, '[0.1, 1000]'"]),
}


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_c1_a_value_the_driver_folds_is_named_never_proven(pg, fmt):
    tables = _rehearse(pg(NAMED), fmt)
    for group, (_ddl, _rows, said) in NAMED.items():
        table = tables[group]
        assert table["status"] == "not_proven" and said in table["reason"], (group, table["status"],
                                                                              table["reason"])


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_h2_an_array_number_whose_scale_the_extract_dropped_differs(pg, fmt):
    tables = _rehearse(pg({g: spec[:2] for g, spec in DIFFERS.items()}), fmt)
    for group, (_ddl, _rows, column) in DIFFERS.items():
        table = tables[group]
        assert table["status"] == "incomplete", (group, table["status"], table["reason"])
        assert f"column(s) {column}" in table["reason"], table["reason"]


@pytest.mark.parametrize("fmt", ["jsonl", "csv"])
def test_c1_shapes_the_driver_reads_one_to_one_are_still_proven(pg, fmt):
    tables = _rehearse(pg(HONEST), fmt)
    wrong = {g: (t["status"], t["reason"]) for g, t in tables.items() if t["status"] != "proven"}
    assert not wrong, wrong


def test_review5_c1_the_same_tables_saved_under_a_postgresql_family_type_get_identical_verdicts(pg, monkeypatch):
    """Review 5, C-1: a hosted PostgreSQL tile (supabase, amazon_rds_postgresql) is PostgreSQL on the wire."""
    from services import schedule_runner

    shapes = {g: NAMED[g][:2] for g in ("arrlb", "tm24", "jbnum")}
    shapes.update({"numarr": DIFFERS["numarr"][:2], "jbok": HONEST["jbok"]})
    names = pg(shapes)
    resolve = schedule_runner._resolve_connector_for_schedule
    verdicts = {}
    for ctype in ("postgresql", "supabase", "amazon_rds_postgresql"):
        monkeypatch.setattr(schedule_runner, "_resolve_connector_for_schedule",
                            lambda c, s, _t=ctype: {**resolve(c, s), "type": _t})
        verdicts[ctype] = {g: t["status"] for g, t in _rehearse(names, "jsonl").items()}
    assert verdicts["postgresql"] == {"arrlb": "not_proven", "tm24": "not_proven", "jbnum": "not_proven",
                                      "numarr": "incomplete", "jbok": "proven"}, verdicts
    assert verdicts["supabase"] == verdicts["postgresql"] == verdicts["amazon_rds_postgresql"], verdicts
