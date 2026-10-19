"""EXPLAIN shows the query as osagg received it, on one line (0.2.17): in SQL Lab, the values Superset's Jinja put in
it are seen there, with a query that does not parse or plan too (a Jinja value that broke it)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import osagg
from osagg.dbapi import one_line
from osagg.metadata import ID_FIELD, TableMeta, fields_from_mapping

from generic_mapping import MAPPING


def test_one_line_keeps_what_is_quoted():
    sql = ("SELECT APPLICATION,\n\tCOUNT(*) AS n\nFROM jobs\nWHERE \"@timestamp_date\" >= '2026-10-18  00:00:00'  "
           "-- from_dttm\n  AND STATUS_INFO IN ('KO', 'it''s')\nGROUP BY 1")
    assert one_line(sql) == ("SELECT APPLICATION, COUNT(*) AS n FROM jobs WHERE \"@timestamp_date\" >= "
                             "'2026-10-18  00:00:00' /* from_dttm */ AND STATUS_INFO IN ('KO', 'it''s') GROUP BY 1")
    assert one_line("SELECT 1 /* a\n   b */\n") == "SELECT 1 /* a b */"
    assert one_line("SELECT 'x -- not a comment'") == "SELECT 'x -- not a comment'"
    assert one_line("SELECT 1 -- a */ b") == "SELECT 1 /* a * / b */"


@pytest.fixture()
def cursor():
    fields = fields_from_mapping({"jobs": {"mappings": MAPPING}})
    fields["_id"] = ID_FIELD
    meta = TableMeta("jobs", ["jobs"], fields)
    conn = osagg.connect(host="opensearch.invalid", timezone="Europe/Paris", label_source="none")
    conn.table_meta = lambda name: meta if name == "jobs" else None
    conn.transport = SimpleNamespace(kind="test")              # no request: the plan is made from the mapping
    return conn.cursor()


def _plan(cur):
    return [r[0] for r in cur.fetchall()]


def test_explain_says_the_query_received_on_one_line(cursor):
    cursor.execute("EXPLAIN SELECT APPLICATION, COUNT(*) AS n\nFROM jobs\nWHERE \"@timestamp_date\" >= "
                   "'2026-10-18T00:00:00'\n  AND STATUS_INFO IN ('KO', 'ABORTED')\nGROUP BY APPLICATION;")
    rows = _plan(cursor)
    assert rows[0].startswith("-- osagg ") and "test transport" in rows[0]
    assert rows[1] == "-- the query as osagg received it (with the values of its Jinja), on one line:"
    assert rows[2] == ("SELECT APPLICATION, COUNT(*) AS n FROM jobs WHERE \"@timestamp_date\" >= "
                       "'2026-10-18T00:00:00' AND STATUS_INFO IN ('KO', 'ABORTED') GROUP BY APPLICATION")
    assert any(r.startswith("-- 1. OpenSearch") for r in rows) and any("DuckDB" in r for r in rows)


def test_a_query_that_does_not_parse_is_said_with_its_line(cursor):
    cursor.execute("EXPLAIN SELECT COUNT(*) FROM jobs\nWHERE STATUS_INFO =\n  AND APPLICATION = 'X'")  # an empty value
    rows = _plan(cursor)
    assert rows[2] == "SELECT COUNT(*) FROM jobs WHERE STATUS_INFO = AND APPLICATION = 'X'"
    assert len(rows) > 3 and all(r.startswith("-- ERROR, not planned: ") for r in rows[3:])
    assert "SQL syntax error" in rows[3] and not any("\x1b[" in r for r in rows)
    assert any(">>AND<<" in r for r in rows[3:])                                  # where it broke, pointed at


def test_a_query_that_cannot_be_planned_is_said_with_its_line(cursor):
    cursor.execute("EXPLAIN SELECT SUM(ERROR_EXCEPTION) FROM jobs\nWHERE APPLICATION = 'X'")
    rows = _plan(cursor)
    assert rows[2] == "SELECT SUM(ERROR_EXCEPTION) FROM jobs WHERE APPLICATION = 'X'"
    assert rows[3].startswith("-- ERROR, not planned: ") and "ERROR_EXCEPTION" in " ".join(rows[3:])


def test_a_query_that_does_not_parse_still_fails_when_run(cursor):
    with pytest.raises(osagg.ProgrammingError, match="syntax error"):            # only EXPLAIN says it in rows
        cursor.execute("SELECT COUNT(*) FROM jobs WHERE STATUS_INFO = AND APPLICATION = 'X'")
